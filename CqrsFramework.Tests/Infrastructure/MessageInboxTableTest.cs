﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.InTable;
using CqrsFramework.InMemory;
using System.IO;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Xml.Linq;
using System.Threading;

namespace CqrsFramework.Tests.Infrastructure
{
    [TestClass]
    public class MessageInboxTableTest
    {
        private MockRepository _repo;
        private Mock<IMessageSerializer> _serializer;
        private TestTimeProvider _time;
        private Mock<ITableProvider> _table;

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
            return new TableMessageInbox(_table.Object, _serializer.Object, _time);
        }

        private TableMessageInbox CreateReader()
        {
            return new TableMessageInbox(_table.Object, _serializer.Object, _time);
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
                new TableProviderColumn(1, "deliveron", typeof(long), false),
                new TableProviderColumn(2, "status", typeof(int), false),
                new TableProviderColumn(3, "data", typeof(byte[]), false)
            });
            _table.Setup(t => t.GetRows()).Returns(new TableProviderFilterable(_table.Object));
        }

        private TableProviderRow CreateTableRow()
        {
            return new TableProviderRow(_table.Object, 0, new object[3] { 0, 0, null });
        }

        private TableProviderFilter[] MatchAllMessages()
        {
            return Match.Create<TableProviderFilter[]>(f => f != null && f.Length == 0, () => MatchAllMessages());
        }

        private TableProviderFilter[] MatchNewMessages()
        {
            return Match.Create<TableProviderFilter[]>(
                f => f != null && f.Length == 1 &&
                    f[0].Type == TableProviderFilterType.Exact &&
                    f[0].MinValue is int &&
                    (int)f[0].MinValue == 0 &&
                    f[0].ColumnIndex == 2);
        }

        private TableProviderRow MatchFrom(List<TableProviderRow> list)
        {
            return Match.Create<TableProviderRow>(r => list.Contains(r));
        }

        private void AddMessageRow(List<TableProviderRow> rows, Message message, int status = 0)
        {
            var row = CreateTableRow();
            if (message.Headers.DeliverOn == DateTime.MinValue)
                row[1] = message.Headers.CreatedOn.Ticks;
            else
                row[1] = message.Headers.DeliverOn.Ticks;
            row[2] = status;
            row[3] = SerializeMessage(message);
            rows.Add(row);
        }
        
        private void RemoveReceivedRowFromNewMessages(List<TableProviderRow> rows, TableProviderRow row)
        {
            if (row.Get<int>("status") == 2)
                rows.Remove(row);
            else
                throw new InvalidOperationException("Message must have status 2");
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
            Assert.AreEqual(new DateTime(2013, 6, 3, 17, 22, 54, 124).Ticks, createdRow.Get<long>("deliveron"), "DeliverOn");
            Assert.AreEqual(0, createdRow.Get<int>("status"), "Status");
            AssertExtension.AreEqual(expectedBytes, createdRow.Get<byte[]>("data"));
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
            var putMessage = DeserializeMessage(createdRow.Get<byte[]>("data"));
            Assert.AreNotEqual(Guid.Empty, putMessage.Headers.MessageId, "MessageId");
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

#if false
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
            var name1 = FileMessageInboxReader.CreateQueueName(message1, 1);
            var name2 = FileMessageInboxReader.CreateQueueName(message2, 1);
            var name3 = FileMessageInboxReader.CreateQueueName(message3, 1);
            var name4 = FileMessageInboxReader.CreateQueueName(message4, 1);
            _memoryDir.SetContents(name1, SerializeMessage(message1));
            _memoryDir.SetContents(name2, SerializeMessage(message2));
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _directory.Setup(d => d.GetStreams()).Returns(_memoryDir.GetStreams).Verifiable();
            _directory.Setup(d => d.Open(name1, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            _directory.Setup(d => d.Open(name2, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            _directory.Setup(d => d.Open(name3, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            _directory.Setup(d => d.Open(name4, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            var inbox = CreateReader();
            var received1 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            _memoryDir.SetContents(name3, SerializeMessage(message3));
            var received2 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            var received3 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            var task4 = inbox.ReceiveAsync(CancellationToken.None);
            _memoryDir.SetContents(name4, SerializeMessage(message4));
            _time.ChangeTime(_time.Get().AddMilliseconds(300));
            var received4 = task4.GetAwaiter().GetResult();
            _serializer.Verify(s => s.Deserialize(It.IsAny<byte[]>()), Times.Exactly(4));
            _repo.Verify();
            AssertExtension.AreEqual(message1, received1);
            AssertExtension.AreEqual(message2, received2);
            AssertExtension.AreEqual(message3, received3);
            AssertExtension.AreEqual(message4, received4);
        }
#endif

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
            AddMessageRow(newMessages, originalMessage);
            _serializer.Setup(s => s.Serialize(It.IsAny<Message>())).Returns<Message>(SerializeMessage);
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(() => newMessages.ToArray()).Verifiable();
            _table.Setup(t => t.Update(MatchFrom(newMessages)))
                .Callback<TableProviderRow>(r => { RemoveReceivedRowFromNewMessages(newMessages, r); receivedMessages.Add(r); })
                .Verifiable();
            _table.Setup(t => t.Delete(MatchFrom(receivedMessages))).Verifiable();
            _table.Setup(t => t.Insert(It.IsAny<TableProviderRow>())).Callback<TableProviderRow>(r => putMessage = r).Verifiable();
            var inbox = CreateReader();
            var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            received.Headers.DeliverOn = received.Headers.CreatedOn.AddSeconds(40);
            received.Headers.RetryNumber = 3;
            inbox.Put(received);
            _repo.Verify();
            var stored = DeserializeMessage(putMessage.Get<byte[]>("data"));
            Assert.AreEqual(1, putMessage.Get<int>("status"));
            Assert.AreEqual(3, received.Headers.RetryNumber);
            Assert.AreEqual(_time.Get().AddSeconds(40), received.Headers.DeliverOn);
            Assert.AreEqual("Message for retry", received.Payload);
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutNewMessageToQueue()
        {
            var message = new Message("New message");
            TableProviderRow putMessage = null;
            _serializer.Setup(s => s.Serialize(message)).Returns<Message>(SerializeMessage);
            _table.Setup(t => t.GetRows(MatchAllMessages())).Returns(new TableProviderRow[0]);
            _table.Setup(t => t.Insert(It.IsAny<TableProviderRow>())).Callback<TableProviderRow>(r => putMessage = r).Verifiable();
            var inbox = CreateReader();
            inbox.Put(message);
            _repo.Verify();
            Assert.AreNotEqual(DateTime.MinValue, message.Headers.CreatedOn, "Creation date");
            Assert.AreNotEqual(Guid.Empty, message.Headers.MessageId, "MessageId");
        }
    }
}
