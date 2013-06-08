using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.InFile;
using CqrsFramework.InMemory;
using System.IO;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace CqrsFramework.Tests.Infrastructure
{
    [TestClass]
    public class MessageInboxFilesystemTest
    {
        private MockRepository _repo;
        private Mock<IStreamProvider> _directory;
        private Mock<IMessageSerializer> _serializer;
        private MemoryStreamProvider _memoryDir;
        private TestTimeProvider _time;

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

        private FileMessageInboxWriter CreateWriter()
        {
            return new FileMessageInboxWriter(_directory.Object, _serializer.Object, _time);
        }

        private FileMessageInboxReader CreateReader()
        {
            return new FileMessageInboxReader(_directory.Object, _serializer.Object, _time);
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
            _directory = _repo.Create<IStreamProvider>();
            _serializer = _repo.Create<IMessageSerializer>();
            _time = new TestTimeProvider(new DateTime(2013, 6, 5, 15, 2, 8, DateTimeKind.Utc));
            _memoryDir = new MemoryStreamProvider();
        }

        [TestMethod]
        public void PutCreatesNewStream()
        {
            var message = new Message("Hello world");
            message.Headers.CreatedOn = new DateTime(2013, 6, 3, 17, 22, 54, 124);
            message.Headers.MessageId = new Guid("a7cf1cd4-88c8-43a3-b4c2-02133fc15b7a");
            message.Headers.CorellationId = new Guid("006d65ca-ec31-4b75-91b4-65fb0095a28a");

            var messageStream = new MemoryStream();
            var expectedBytes = SerializeMessage(message);

            _directory
                .Setup(d => d.Open("20130603172254124000.a7cf1cd488c843a3b4c202133fc15b7a.queuemessage", FileMode.CreateNew))
                .Returns(messageStream).Verifiable();
            _serializer.Setup(d => d.Serialize(message)).Returns<Message>(SerializeMessage).Verifiable();

            var inbox = CreateWriter();
            inbox.Put(message);
            _repo.Verify();

            AssertExtension.AreEqual(expectedBytes, messageStream.ToArray());
        }

        [TestMethod]
        public void PutHandlesMessageWithoutHeaders()
        {
            var message = new Message("Hello world");

            var messageStream = new MemoryStream();
            string usedName = null;

            _directory
                .Setup(d => d.Open(It.IsAny<string>(), FileMode.CreateNew))
                .Returns(messageStream)
                .Callback<string, FileMode>((s, m) => usedName = s);
            _serializer.Setup(d => d.Serialize(message)).Returns<Message>(SerializeMessage);

            var inbox = CreateWriter();
            inbox.Put(message);

            var match = Regex.Match(usedName, @"^([0-9]{17})([0-9]{3}).([0-9a-z]{32}).queuemessage");
            if (!match.Success)
                Assert.Fail("Stream name completely invalid: {0}", usedName);
            Assert.AreEqual("20130605150208000", match.Groups[1].Value, "Date");
            Assert.AreEqual("000", match.Groups[2].Value, "Sequence ID");
            Assert.AreNotEqual(Guid.Empty.ToString("N"), match.Groups[3].Value, "Message id");
        }

        [TestMethod]
        public void PutMessagesInSameSecondHaveDifferentSequenceNumber()
        {
            var usedNames = new List<string>();

            _directory
                .Setup(d => d.Open(It.IsAny<string>(), FileMode.CreateNew))
                .Returns(() => new MemoryStream())
                .Callback<string, FileMode>((s, m) => usedNames.Add(s));
            _serializer
                .Setup(d => d.Serialize(It.IsAny<Message>()))
                .Returns<Message>(SerializeMessage);

            var inbox = CreateWriter();
            inbox.Put(new Message("Hello world"));
            inbox.Put(new Message("See ya"));

            var regex = new Regex(@"^([0-9]{17})([0-9]{3}).([0-9a-z]{32}).queuemessage");
            var usedSequences = new HashSet<string>();
            foreach (var usedName in usedNames)
            {
                var match = regex.Match(usedName);
                if (!match.Success)
                    Assert.Fail("Stream name completely invalid: {0}", usedName);
                Assert.AreEqual("20130605150208000", match.Groups[1].Value, "Date");
                if (!usedSequences.Add(match.Groups[2].Value))
                    Assert.Fail("Sequence ID {0} was already used", match.Groups[2].Value);
                Assert.AreNotEqual(Guid.Empty.ToString("N"), match.Groups[3].Value, "Message id");
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveWaitsForPutWhenEmpty()
        {
            var originalMessage = BuildMessage("Hello world");
            var streamName = FileMessageInboxReader.CreateQueueName(originalMessage, 0);
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _directory.Setup(d => d.GetStreams()).Returns(_memoryDir.GetStreams);
            _directory.Setup(d => d.Open(streamName, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open);
            var inbox = CreateReader();
            var task = inbox.ReceiveAsync(CancellationToken.None);
            Assert.IsFalse(task.IsCompleted, "Complete at start");
            _memoryDir.SetContents(FileMessageInboxReader.CreateQueueName(originalMessage, 0), SerializeMessage(originalMessage));
            _time.ChangeTime(_time.Get().AddMilliseconds(300));
            var received = task.GetAwaiter().GetResult();
            AssertExtension.AreEqual(originalMessage, received);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceivesMessageThatWasPresentWhenCreating()
        {
            var originalMessage = BuildMessage("Hello world");
            var streamName = FileMessageInboxReader.CreateQueueName(originalMessage, 0);
            _memoryDir.SetContents(streamName, SerializeMessage(originalMessage));
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _directory.Setup(d => d.GetStreams()).Returns(new string[] { streamName }).Verifiable();
            _directory.Setup(d => d.Open(streamName, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
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
            var streamName = FileMessageInboxReader.CreateQueueName(originalMessage, 1);
            _serializer.Setup(d => d.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _directory.Setup(d => d.GetStreams()).Returns(new string[] { streamName }).Verifiable();
            _directory.Setup(d => d.Open(streamName, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            var inbox = CreateReader();
            _memoryDir.SetContents(streamName, SerializeMessage(originalMessage));
            var receivedMessage = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            _repo.Verify();
            AssertExtension.AreEqual(originalMessage, receivedMessage);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveIsCancellable()
        {
            var cancel = new CancellationTokenSource();
            _directory.Setup(d => d.GetStreams()).Returns(new string[0]);
            var inbox = CreateReader();
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

        [TestMethod]
        [Timeout(1000)]
        public void DeleteFromQueue()
        {
            var originalMessage = BuildMessage("Message for deletion");
            var streamName = FileMessageInboxReader.CreateQueueName(originalMessage, 1);
            _memoryDir.SetContents(streamName, SerializeMessage(originalMessage));
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage);
            _directory.Setup(d => d.GetStreams()).Returns(_memoryDir.GetStreams).Verifiable();
            _directory.Setup(d => d.Open(streamName, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            _directory.Setup(d => d.Delete(streamName)).Callback<string>(_memoryDir.Delete).Verifiable();
            var inbox = CreateReader();
            var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            inbox.Delete(received);
            _repo.Verify();
            Assert.IsTrue(_memoryDir.GetContents(streamName) == null);
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutModifiedMessageBackToQueue()
        {
            var originalMessage = BuildMessage("Message for retry");
            var streamName = FileMessageInboxReader.CreateQueueName(originalMessage, 3);
            _memoryDir.SetContents(streamName, SerializeMessage(originalMessage));
            _serializer.Setup(s => s.Serialize(It.IsAny<Message>())).Returns<Message>(SerializeMessage);
            _serializer.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(DeserializeMessage).Verifiable();
            _directory.Setup(d => d.GetStreams()).Returns(_memoryDir.GetStreams).Verifiable();
            _directory.Setup(d => d.Open(streamName, FileMode.Open)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            _directory.Setup(d => d.Open(streamName, FileMode.Create)).Returns<string, FileMode>(_memoryDir.Open).Verifiable();
            var inbox = CreateReader();
            var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
            received.Headers.DeliverOn = received.Headers.CreatedOn.AddSeconds(40);
            received.Headers.RetryNumber = 3;
            inbox.Put(received);
            _repo.Verify();
            var stored = DeserializeMessage(_memoryDir.GetContents(streamName));
            Assert.AreEqual(3, received.Headers.RetryNumber);
            Assert.AreEqual(_time.Get().AddSeconds(40), received.Headers.DeliverOn);
            Assert.AreEqual("Message for retry", received.Payload);
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutNewMessageToQueue()
        {
            var message = new Message("New message");
            _serializer.Setup(s => s.Serialize(message)).Returns<Message>(SerializeMessage);
            _directory
                .Setup(d => d.Open(
                    It.IsAny<string>(),
                    It.Is<FileMode>(m => m == FileMode.Create || m == FileMode.CreateNew)))
                .Returns<string, FileMode>(_memoryDir.Open)
                .Verifiable();
            var inbox = CreateReader();
            inbox.Put(message);
            _repo.Verify();
            Assert.AreNotEqual(DateTime.MinValue, message.Headers.CreatedOn, "Creation date");
            Assert.AreNotEqual(Guid.Empty, message.Headers.MessageId, "MessageId");
            var streamName = _memoryDir.GetStreams().FirstOrDefault();
            Assert.IsNotNull(streamName, "New item created");
        }
    }
}
