using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.InFile;
using System.IO;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Xml.Linq;
using System.Text.RegularExpressions;

namespace CqrsFramework.Tests.Infrastructure
{
    [TestClass]
    public class MessageInboxFilesystemTest
    {
        private MockRepository _repo;
        private Mock<IStreamProvider> _directory;
        private Mock<IMessageSerializer> _serializer;
        private Mock<ITimeProvider> _time;
        private DateTime _now;

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

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _directory = _repo.Create<IStreamProvider>();
            _serializer = _repo.Create<IMessageSerializer>();
            _time = _repo.Create<ITimeProvider>();
            _time.Setup(t => t.Get()).Returns(() => _now);
            _now = new DateTime(2013, 6, 5, 15, 2, 8, DateTimeKind.Utc);
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

            var inbox = new FileMessageInboxWriter(_directory.Object, _serializer.Object, _time.Object);
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

            var inbox = new FileMessageInboxWriter(_directory.Object, _serializer.Object, _time.Object);
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

            var inbox = new FileMessageInboxWriter(_directory.Object, _serializer.Object, _time.Object);
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

        [TestMethod, Ignore]
        public void EnumeratesStreamsOnReaderCreationButDoesNotReadThemUntilReceivingStarts()
        {
            
        }

        [TestMethod, Ignore]
        public void ReceiveWaitsForPutWhenEmpty()
        {
        }

        [TestMethod, Ignore]
        public void ReceivesMessageThatWasPresentWhenCreating()
        {
        }

        [TestMethod, Ignore]
        public void ReceivesMessageThatWasPutJustBeforeReceive()
        {
        }

        [TestMethod, Ignore]
        public void ReceiveIsCancellable()
        {
        }

        [TestMethod, Ignore]
        public void ReceivesInOrder()
        {
        }

        [TestMethod, Ignore]
        public void DeleteFromQueue()
        {
        }

        [TestMethod, Ignore]
        public void PutModifiedMessageBackToQueue()
        {
        }

        [TestMethod, Ignore]
        public void PutNewMessageToQueue()
        {
        }
    }
}
