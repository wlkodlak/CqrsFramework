using CqrsFramework.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Moq;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class MessageBodySerializerProtobufTest
    {
        private ProtobufMessageBodySerializer _serializer;
        private Mock<IMessageTypeResolver> _resolver;
        private TestMessage1 _contents1;
        private TestMessage2 _contents2;

        [TestInitialize]
        public void Initialize()
        {
            var knownTypes = new Type[] { typeof(TestMessage1), typeof(TestMessage2) };
            _resolver = new Mock<IMessageTypeResolver>(MockBehavior.Strict);
            _resolver.Setup(r => r.GetName(typeof(TestMessage1))).Returns("TestMessage1Typename").Verifiable();
            _resolver.Setup(r => r.GetName(typeof(TestMessage2))).Returns("TestMessage2Type").Verifiable();
            _serializer = new ProtobufMessageBodySerializer(knownTypes, _resolver.Object);
            _contents1 = new TestMessage1() { IntValue = 44, StringValue = "Hello" };
            _contents2 = new TestMessage2() { DateValue = new DateTime(2013, 8, 5), StringValue = "White Rabbit" };
        }

        [TestMethod]
        public void SerializePayload()
        {
            using (var stream = new MemoryStream())
            {
                Serializer.Serialize<TestMessage1>(stream, _contents1);
                var expected = stream.ToArray();
                var type = typeof(TestMessage1);
                var headers = new MessageHeaders();
                var serialized = _serializer.Serialize(_contents1, headers);
                _resolver.Verify();
                AssertExtension.AreEqual(expected, serialized);
            }
        }

        [TestMethod]
        public void SerializeCreatesHeaders()
        {
            using (var stream = new MemoryStream())
            {
                var headers = new MessageHeaders();
                var serialized = _serializer.Serialize(_contents2, headers);
                _resolver.Verify();
                Assert.AreEqual(serialized.Length, headers.PayloadLength, "Length");
                Assert.AreEqual("TestMessage2Type", headers.PayloadType, "Typename");
            }
        }

        [TestMethod]
        public void DeserializePayload()
        {
            using (var stream = new MemoryStream())
            {
                Serializer.Serialize<TestMessage2>(stream, _contents2);
                var serialized = stream.ToArray();

                var headers = new MessageHeaders();
                headers.PayloadLength = serialized.Length;
                headers.PayloadType = "TestMessage2Type";

                var deserialized = _serializer.Deserialize(serialized, headers);

                AssertExtension.AreEqual(_contents2, deserialized);
            }
        }

        [TestMethod]
        public void DeserializeDropsHeaders()
        {
            using (var stream = new MemoryStream())
            {
                Serializer.Serialize<TestMessage2>(stream, _contents2);
                var serialized = stream.ToArray();

                var headers = new MessageHeaders();
                headers["PayloadLength"] = serialized.Length.ToString();
                headers["PayloadType"] = "TestMessage2Type";

                var deserialized = _serializer.Deserialize(serialized, headers);

                Assert.IsNull(headers.PayloadFormat);
                Assert.IsNull(headers.PayloadType);
                Assert.AreEqual(0, headers.PayloadLength);
            }
        }

        [TestMethod]
        public void Roundtrip1()
        {
            var headers = new MessageHeaders();
            headers.MessageId = Guid.NewGuid();
            var serialized = _serializer.Serialize(_contents1, headers);
            var deserialized = _serializer.Deserialize(serialized, headers);
            AssertExtension.AreEqual(_contents1, deserialized as TestMessage1);
        }

        [TestMethod]
        public void Roundtrip2()
        {
            var headers = new MessageHeaders();
            headers.MessageId = Guid.NewGuid();
            var serialized = _serializer.Serialize(_contents2, headers);
            var deserialized = _serializer.Deserialize(serialized, headers);
            AssertExtension.AreEqual(_contents2, deserialized as TestMessage2);
        }

        [DataContract(Namespace = "")]
        public class TestMessage1
        {
            [DataMember(Order = 1)]
            public string StringValue;
            [DataMember(Order = 4)]
            public int IntValue;
        }

        [DataContract(Namespace = "")]
        public class TestMessage2
        {
            [DataMember(Order = 2)]
            public DateTime DateValue;
            [DataMember(Order = 4)]
            public string StringValue;
        }
    }
}
