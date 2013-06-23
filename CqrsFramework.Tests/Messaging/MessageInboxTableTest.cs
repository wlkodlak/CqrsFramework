using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Xml.Linq;
using System.Threading;
using CqrsFramework.Serialization;
using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class MessageInboxTableTest
    {
        private MockRepository _repo;
        private Mock<IMessageSerializer> _serializer;
        private TestTimeProvider _time;
        private Mock<ITableProvider> _table;
        private string _queueName = "queue";

        private byte[] SerializeMessage(Message message)
        {
            var xml = new XElement("Message",
                new XElement("Headers", message.Headers.Select(h => new XElement("Header", new XAttribute("Name", h.Name), h.Value))),
                new XElement("Body", message.Payload.ToString()));
            using (var stream = new MemoryStream())
            {
                xml.Save(stream);
                return stream.ToArray();
            }
        }

        private Message DeserializeMessage(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                var xml = XElement.Load(stream);
                var message = new Message(xml.Element("Body").Value);
                foreach (var header in xml.Element("Headers").Elements("Header"))
                    message.Headers[header.Attribute("Name").Value] = header.Value;
                return message;
            }
        }

        private TableMessageInbox CreateWriter()
        {
            return new TableMessageInbox(_table.Object, _queueName, _serializer.Object, _time);
        }

        private TableMessageInbox CreateReader()
        {
            return new TableMessageInbox(_table.Object, _queueName, _serializer.Object, _time);
        }

        private Message BuildMessage(string contents)
        {
            var message = new Message(contents);
            message.Headers.CreatedOn = _time.Get();
            message.Headers.MessageId = Guid.NewGuid();
            return message;
        }

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _table = _repo.Create<ITableProvider>();
            _serializer = _repo.Create<IMessageSerializer>();
            _time = new TestTimeProvider(new DateTime(2013, 6, 5, 15, 2, 8, DateTimeKind.Utc));
            _table.Setup(t => t.NewRow()).Returns(CreateTableRow);
            _table.Setup(t => t.GetColumns()).Returns(new TableProviderColumn[]
            {
                new TableProviderColumn(1, "queue", typeof(string), false),
                new TableProviderColumn(2, "messageid", typeof(string), false),
                new TableProviderColumn(3, "deliveron", typeof(long), false),
                new TableProviderColumn(4, "status", typeof(int), false),
                new TableProviderColumn(5, "data", typeof(byte[]), false),
            });
            _table.Setup(t => t.GetRows()).Returns(new TableProviderFilterable(_table.Object));
        }

        private TableProviderRow CreateTableRow()
        {
            return new TableProviderRow(_table.Object, 0, new object[5] { null, Guid.Empty.ToString("D"), 0, 0, null });
        }

        private TableProviderFilter[] MatchAllMessages()
        {
            return Match.Create<TableProviderFilter[]>(f => MatchAllMessagesPredicate(f), () => MatchAllMessages());
        }

        private bool MatchAllMessagesPredicate(TableProviderFilter[] filters)
        {
            if (filters == null)
                return false;
            bool queueValid = false;
            foreach (var filter in filters)
            {
                if (filter.ColumnIndex == 4)
                    return false;
                else if (filter.ColumnIndex == 1)
                {
                    if (filter.Type != TableProviderFilterType.Exact || (string)filter.MinValue != _queueName)
                        return false;
                    queueValid = true;
                }
            }
            return queueValid;
        }

        private TableProviderFilter[] MatchNewMessages()
        {
            return Match.Create<TableProviderFilter[]>(f => MatchNewMessagesPredicate(f), () => MatchNewMessages());
        }

        private bool MatchNewMessagesPredicate(TableProviderFilter[] filters)
        {
            if (filters == null)
                return false;
            bool statusValid = false;
            bool queueValid = false;
            foreach (var filter in filters)
            {
                if (filter.ColumnIndex == 4)
                {
                    if (filter.Type != TableProviderFilterType.Exact || (int)filter.MinValue != 0)
                        return false;
                    statusValid = true;
                }
                else if (filter.ColumnIndex == 1)
                {
                    if (filter.Type != TableProviderFilterType.Exact || (string)filter.MinValue != _queueName)
                        return false;
                    queueValid = true;
                }
            }
            return statusValid && queueValid;
        }

        private TableProviderRow MatchFrom(List<TableProviderRow> list)
        {
            return Match.Create<TableProviderRow>(r => list.Contains(r));
        }

        private void AddMessageRow(List<TableProviderRow> rows, Message message, int status = 0)
        {
            var row = CreateTableRow();
            row["queue"] = _queueName;
            row["messageid"] = message.Headers.MessageId.ToString("D");
            if (message.Headers.DeliverOn == DateTime.MinValue)
                row["deliveron"] = message.Headers.CreatedOn.Ticks;
            else
                row["deliveron"] = message.Headers.DeliverOn.Ticks;
            row["status"] = status;
            row["data"] = SerializeMessage(message);
            rows.Add(row);
        }
        
        private void RemoveReceivedRowFromNewMessages(List<TableProviderRow> rows, TableProviderRow row)
        {
            if (row.Get<int>("status") != 0)
                rows.Remove(row);
            else
                throw new InvalidOperationException("Message must not have status 0");
        }


        [TestMethod]
        public void PutCreatesNewStream()
        {
            var message = new Message("Hello world");
            message.Headers.CreatedOn = new DateTime(2013, 6, 3, 17, 22, 54, 124);
            message.Headers.MessageId = new Guid("a7cf1cd4-88c8-43a3-b4c2-02133fc15b7a");
            message.Headers.CorellationId = new Guid("006d65ca-ec31-4b75-91b4-65fb0095a28a");

            var expectedBytes = SerializeMessage(message);
            TableProviderRow createdRow = null;

            _table.Setup(t => t.Insert(It.IsAny<TableProviderRow>())).Callback<TableProviderRow>(r => createdRow = r).Verifiable();
            _serializer.Setup(d => d.Serialize(message)).Returns<Message>(SerializeMessage).Verifiable();

            var inbox = CreateWriter();
            inbox.Put(message);
            _repo.Verify();

            Assert.IsNotNull(createdRow, "Row inserted");
            Assert.AreEqual(message.Headers.MessageId.ToString("D"), createdRow.Get<string>("messageid"), "MessageId");
            Assert.AreEqual(new DateTime(2013, 6, 3, 17, 22, 54, 124).Ticks, createdRow.Get<long>("deliveron"), "DeliverOn");
            Assert.AreEqual(0, createdRow.Get<int>("status"), "Status");
            AssertExtension.AreEqual(expectedBytes, createdRow.Get<byte[]>("data"), "Data");
            Assert.AreEqual(_queueName, createdRow.Get<string>("queue"), "Queue");
        }

        [TestMethod]
        public void PutHandlesMessageWithoutHeaders()
        {
            var message = new Message("Hello world");

            TableProviderRow createdRow = null;
            _table.Setup(t => t.Insert(It.IsAny<TableProviderRow>())).Callback<TableProviderRow>(r => createdRow = r).Verifiable();
            _serializer.Setup(d => d.Serialize(message)).Returns<Message>(SerializeMessage);

            var inbox = CreateWriter();
            inbox.Put(message);

            Assert.IsNotNull(createdRow, "Row inserted");
            Assert.AreEqual(_time.Get().Ticks, createdRow.Get<long>("deliveron"), "Table deliver-on");
            Assert.AreEqual(0, createdRow.Get<int>("status"), "Status");
            Assert.AreEqual(_queueName, createdRow.Get<string>("queue"), "Queue");
            var putMessage = DeserializeMessage(createdRow.Get<byte[]>("data"));
            Assert.AreNotEqual(Guid.Empty, putMessage.Headers.MessageId, "MessageId");
            Assert.AreEqual(putMessage.Headers.MessageId.ToString("D"), createdRow.Get<string>("messageid"), "Table MessageId");
            Assert.AreEqual(_time.Get(), putMessage.Headers.CreatedOn, "CreatedOn");
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveWaitsForPutWhenEmpty()
        {
            var originalMessage = BuildMessage("Hello world");
            var newMessages = new List<TableProviderRow>();
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(new TableProviderRow[0]).Verifiable();
            _table.Setup(t => t.GetRows(MatchNewMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages))).Callback<TableProviderRow>(r => RemoveReceivedRowFromNewMessages(newMessages, r)).Verifiable();
            var inbox = CreateReader();
            var task = inbox.ReceiveAsync(CancellationToken.None);
            Assert.IsFalse(task.IsCompleted, "Complete at start");
            AddMessageRow(newMessages, originalMessage);
            _time.ChangeTime(_time.Get().AddMilliseconds(300));
            var received = task.GetAwaiter().GetResult();
            AssertExtension.AreEqual(originalMessage, received);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceivesMessageThatWasPresentWhenCreating()
        {
            var originalMessage = BuildMessage("Hello world");
            var newMessages = new List<TableProviderRow>();
            AddMessageRow(newMessages, originalMessage);
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages))).Callback<TableProviderRow>(r => RemoveReceivedRowFromNewMessages(newMessages, r)).Verifiable();
            var inbox = CreateReader();
            var receivedMessage = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            _repo.Verify();
            AssertExtension.AreEqual(originalMessage, receivedMessage);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceivesMessageThatWasPutJustBeforeReceive()
        {
            var originalMessage = BuildMessage("Fresh message");
            var newMessages = new List<TableProviderRow>();
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages))).Callback<TableProviderRow>(r => RemoveReceivedRowFromNewMessages(newMessages, r)).Verifiable();
            var inbox = CreateReader();
            AddMessageRow(newMessages, originalMessage);
            var receivedMessage = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            _repo.Verify();
            AssertExtension.AreEqual(originalMessage, receivedMessage);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveIsCancellable()
        {
            var cancel = new CancellationTokenSource();
            var inbox = CreateReader();
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(new TableProviderRow[0]).Verifiable();
            var task = inbox.ReceiveAsync(cancel.Token);
            Assert.IsFalse(task.IsCompleted, "Complete at start");
            cancel.Cancel();
            try
            {
                task.GetAwaiter().GetResult();
                Assert.Fail("Expected cancel");
            }
            catch (OperationCanceledException)
            {
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceivesInOrder()
        {
            var message1 = BuildMessage("Message1");
            message1.Headers.CreatedOn += TimeSpan.FromSeconds(-3);
            var message2 = BuildMessage("Message2");
            message2.Headers.CreatedOn += TimeSpan.FromSeconds(-2);
            var message3 = BuildMessage("Message3");
            message3.Headers.CreatedOn += TimeSpan.FromSeconds(-1);
            var message4 = BuildMessage("Message3");
            message4.Headers.CreatedOn += TimeSpan.FromSeconds(1);
            var messagesTable = new List<TableProviderRow>();
            var receivedMessages = new HashSet<TableProviderRow>();
            AddMessageRow(messagesTable, message1, 2);
            AddMessageRow(messagesTable, message2, 1);
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _table
                .Setup(t => t.GetRows(MatchAllMessages()))
                .Returns(() => messagesTable.ToArray())
                .Verifiable();
            _table
                .Setup(t => t.GetRows(MatchNewMessages()))
                .Returns(() => messagesTable.Where(r => r.Get<int>("status") < 1).ToArray())
                .Verifiable();
            _table
                .Setup(t => t.Update(It.Is<TableProviderRow>(r => messagesTable.Contains(r) && r.Get<int>("status") != 0)))
                .Callback<TableProviderRow>(r =>
                {
                    if (!receivedMessages.Add(r))
                        throw new InvalidOperationException("Row was already updated");
                })
                .Verifiable();
            var inbox = CreateReader();
            var received1 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            AddMessageRow(messagesTable, message3);
            var received2 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            var received3 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            var task4 = inbox.ReceiveAsync(CancellationToken.None);
            AddMessageRow(messagesTable, message4);
            _time.ChangeTime(_time.Get().AddMilliseconds(300));
            var received4 = task4.GetAwaiter().GetResult();
            _serializer.Verify(s => s.Deserialize(It.IsAny<byte[]>()), Times.Exactly(4));
            _repo.Verify();
            AssertExtension.AreEqual(message1, received1);
            AssertExtension.AreEqual(message2, received2);
            AssertExtension.AreEqual(message3, received3);
            AssertExtension.AreEqual(message4, received4);
        }

        [TestMethod]
        [Timeout(1000)]
        public void DeleteFromQueue()
        {
            var originalMessage = BuildMessage("Message for deletion");
            var newMessages = new List<TableProviderRow>();
            var receivedMessages = new List<TableProviderRow>();
            AddMessageRow(newMessages, originalMessage);
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages)))
                .Callback<TableProviderRow>(r => { RemoveReceivedRowFromNewMessages(newMessages, r); receivedMessages.Add(r); })
                .Verifiable();
            _table.Setup(t => t.Delete(MatchFrom(receivedMessages))).Verifiable();
            var inbox = CreateReader();
            var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            inbox.Delete(received);
            _repo.Verify();
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutModifiedMessageBackToQueue()
        {
            var originalMessage = BuildMessage("Message for retry");
            var newMessages = new List<TableProviderRow>();
            var receivedMessages = new List<TableProviderRow>();
            TableProviderRow putMessage = null;
            bool receivedMessageDropped = false;
            AddMessageRow(newMessages, originalMessage);
            _serializer.Setup(s => s.Serialize(It.IsAny<Message>())).Returns<Message>(SerializeMessage);
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages)))
                .Callback<TableProviderRow>(r => { RemoveReceivedRowFromNewMessages(newMessages, r); receivedMessages.Add(r); })
                .Verifiable();
            _table
                .Setup(t => t.Delete(MatchFrom(receivedMessages)))
                .Callback<TableProviderRow>(m => receivedMessageDropped = true);
            _table
                .Setup(t => t.Insert(It.IsAny<TableProviderRow>()))
                .Callback<TableProviderRow>(r => putMessage = r);
            _table
                .Setup(t => t.Update(MatchFrom(receivedMessages)))
                .Callback<TableProviderRow>(r => { putMessage = r; receivedMessageDropped = true; });
            var inbox = CreateReader();
            var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            var modified = new Message(received.Payload);
            modified.Headers.CopyFrom(received.Headers);
            modified.Headers.DeliverOn = received.Headers.CreatedOn.AddSeconds(40);
            modified.Headers.RetryNumber = 3;
            inbox.Put(modified);
            _repo.Verify();
            Assert.IsTrue(receivedMessageDropped, "Original deleted or replaced");
            Assert.IsNotNull(putMessage, "New version of message saved");
            var stored = DeserializeMessage(putMessage.Get<byte[]>("data"));
            Assert.AreEqual(0, putMessage.Get<int>("status"), "Status");
            Assert.AreEqual(3, stored.Headers.RetryNumber, "Retry");
            Assert.AreEqual(_time.Get().AddSeconds(40), stored.Headers.DeliverOn, "DeliverOn");
            Assert.AreEqual(_time.Get().AddSeconds(40).Ticks, putMessage.Get<long>("deliveron"), "Delivery column");
            Assert.AreEqual("Message for retry", stored.Payload, "Payload");
            Assert.AreEqual(received.Headers.MessageId.ToString("D"), putMessage.Get<string>("messageid"), "Table MessageId");
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutNewMessageToQueue()
        {
            var message = new Message("New message");
            TableProviderRow putRow = null;
            _serializer.Setup(s => s.Serialize(message)).Returns<Message>(SerializeMessage);
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(new TableProviderRow[0]);
            _table.Setup(t => t.Insert(It.IsAny<TableProviderRow>())).Callback<TableProviderRow>(r => putRow = r).Verifiable();
            var inbox = CreateReader();
            inbox.Put(message);
            _repo.Verify();
            Assert.IsNotNull(putRow, "Inserted");
            Assert.AreEqual(_queueName, putRow.Get<string>("queue"), "Queue");
            Assert.AreEqual(0, putRow.Get<int>("status"), "Status");
            var putMessage = DeserializeMessage(putRow.Get<byte[]>("data"));
            Assert.AreNotEqual(DateTime.MinValue, putMessage.Headers.CreatedOn, "Creation date");
            Assert.AreNotEqual(Guid.Empty, putMessage.Headers.MessageId, "MessageId");
            Assert.AreEqual("New message", putMessage.Payload, "Payload");
            Assert.AreEqual(putMessage.Headers.MessageId.ToString("D"), putRow.Get<string>("messageid"), "Table MessageId");
        }
    }
}
