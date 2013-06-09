using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class UniversalMessageBodySerializerTest
    {
        private MockRepository _repo;
        private UniversalMessageBodySerializer _serializer;
        private Mock<IMessageBodySerializer> _serializer1;
        private Mock<IMessageBodySerializer> _serializer2;
        private MessageHeaders _headers;

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _serializer1 = _repo.Create<IMessageBodySerializer>();
            _serializer2 = _repo.Create<IMessageBodySerializer>();
            _serializer = new UniversalMessageBodySerializer();
            _headers = new MessageHeaders();
        }

        [TestMethod]
        public void RegisterSerializers()
        {
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            Assert.AreEqual("xml", _serializer.OutputFormat);
        }

        [TestMethod]
        public void DeserializationFromSerializer1()
        {
            var expected = "Hello world";
            var bytes = Encoding.ASCII.GetBytes(expected);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            _headers["PayloadFormat"] = "xml";
            _serializer1.Setup(s => s.Deserialize(bytes, _headers)).Returns(expected).Verifiable(); 
            var actual = (string)_serializer.Deserialize(bytes, _headers);
            _repo.Verify();
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void DeserializationFromSerializer2()
        {
            var expected = "Hello world";
            var bytes = Encoding.ASCII.GetBytes(expected);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            _headers["PayloadFormat"] = "json";
            _serializer2.Setup(s => s.Deserialize(bytes, _headers)).Returns(expected).Verifiable();
            var actual = (string)_serializer.Deserialize(bytes, _headers);
            _repo.Verify();
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void UnknownDeserializer()
        {
            var expected = "Hello world";
            var bytes = Encoding.ASCII.GetBytes(expected);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            _headers["PayloadFormat"] = "protobuf";
            try
            {
                _serializer.Deserialize(bytes, _headers);
                Assert.Fail("Expected KeyNotFoundException");
            }
            catch (KeyNotFoundException)
            {
            }
        }

        [TestMethod]
        public void SerializationCreatesFormatHeader()
        {
            var message = "Hello world";
            var expected = Encoding.ASCII.GetBytes(message);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);

            _serializer1.Setup(s => s.Serialize(message, _headers)).Returns(expected).Verifiable();
            var actual = _serializer.Serialize(message, _headers);
            _repo.Verify();
            Assert.AreEqual(expected, actual, "Data");
            Assert.AreEqual("xml", _headers["PayloadFormat"]);
        }

        [TestMethod]
        public void OutputFormatChangesSerializerForSerialization()
        {
            var message = "Hello world";
            var expected = Encoding.ASCII.GetBytes(message);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            _serializer.OutputFormat = "json";
            _serializer2.Setup(s => s.Serialize(message, _headers)).Returns(expected).Verifiable();
            var actual = _serializer.Serialize(message, _headers);
            _repo.Verify();
            Assert.AreEqual(expected, actual, "Data");
            Assert.AreEqual("json", _headers["PayloadFormat"]);
        }

        [TestMethod]
        public void LinkedWithJustDifferentFormat()
        {
            var message = "Hello world";
            var expected = Encoding.ASCII.GetBytes(message);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            _serializer.RegisterFormat("json", _serializer2.Object);
            var linked = _serializer.CreateLinked("json");
            _serializer2.Setup(s => s.Serialize(message, _headers)).Returns(expected).Verifiable();
            var actual = linked.Serialize(message, _headers);
            _repo.Verify();
            Assert.AreEqual("json", linked.OutputFormat);
            Assert.AreEqual("xml", _serializer.OutputFormat);
            Assert.AreEqual(expected, actual, "Data");
            Assert.AreEqual("json", _headers["PayloadFormat"]);
        }

        [TestMethod]
        public void CreateLinkedAndAddFormatsLater()
        {
            var message = "Hello world";
            var expected = Encoding.ASCII.GetBytes(message);
            _serializer.RegisterFormat("xml", _serializer1.Object);
            var linked = _serializer.CreateLinked();
            _serializer.RegisterFormat("json", _serializer2.Object);
            _serializer2.Setup(s => s.Serialize(message, _headers)).Returns(expected).Verifiable();
            linked.OutputFormat = "json";
            var actual = linked.Serialize(message, _headers);
            _repo.Verify();
            Assert.AreEqual("json", linked.OutputFormat);
            Assert.AreEqual("xml", _serializer.OutputFormat);
            Assert.AreEqual(expected, actual, "Data");
            Assert.AreEqual("json", _headers["PayloadFormat"]);
        }
    }
}
