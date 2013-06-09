using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization;
using CqrsFramework.Serialization;
using Moq;
using ServiceStack.Text;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class MessageBodySerializerJsonTest
    {
        private JsonMessageBodySerializer _serializer;
        private Mock<IMessageTypeResolver> _resolver;
        private TestMessage1 _contents1;
        private TestMessage2 _contents2;
        private Encoding _encoding;

        [TestInitialize]
        public void Initialize()
        {
            var knownTypes = new Type[] { typeof(TestMessage1), typeof(TestMessage2) };
            _resolver = new Mock<IMessageTypeResolver>(MockBehavior.Strict);
            _resolver.Setup(r => r.GetName(typeof(TestMessage1))).Returns("TestMessage1Typename");
            _resolver.Setup(r => r.GetName(typeof(TestMessage2))).Returns("TestMessage2Type");
            _resolver.Setup(r => r.GetType("TestMessage1Typename")).Returns(typeof(TestMessage1));
            _resolver.Setup(r => r.GetType("TestMessage2Type")).Returns(typeof(TestMessage2));
            _serializer = new JsonMessageBodySerializer(knownTypes, _resolver.Object);
            _contents1 = new TestMessage1() { IntValue = 44, StringValue = "Hello" };
            _contents2 = new TestMessage2() { DateValue = new DateTime(2013, 8, 5), StringValue = "White Rabbit" };
            _encoding = new UTF8Encoding(false);
        }

        [TestMethod]
        public void SerializePayload()
        {
            var expectedString = JsonSerializer.SerializeToString(_contents1);
            var expected = _encoding.GetBytes(expectedString);
            var headers = new MessageHeaders();
            var serialized = _serializer.Serialize(_contents1, headers);
            AssertExtension.AreEqual(expected, serialized);
        }

        [TestMethod]
        public void SerializeCreatesHeaders()
        {
            var headers = new MessageHeaders();
            var serialized = _serializer.Serialize(_contents2, headers);
            _resolver.Verify(r => r.GetName(typeof(TestMessage2)));
            Assert.AreEqual(serialized.Length.ToString(), headers["PayloadLength"], "Length");
            Assert.AreEqual("TestMessage2Type", headers["PayloadType"], "Typename");
        }

        [TestMethod]
        public void Deserialize()
        {
            using (var stream = new MemoryStream())
            {
                var expectedString = JsonSerializer.SerializeToString(_contents2);
                var serialized = _encoding.GetBytes(expectedString);

                var headers = new MessageHeaders();
                headers["PayloadLength"] = serialized.Length.ToString();
                headers["PayloadType"] = "TestMessage2Type";

                var deserialized = _serializer.Deserialize(serialized, headers);
                AssertExtension.AreEqual(_contents2, deserialized);
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
            public string StringValue { get; set; }
            [DataMember(Order = 4)]
            public int IntValue { get; set; }
        }

        [DataContract(Namespace = "")]
        public class TestMessage2
        {
            [DataMember(Order = 2)]
            public DateTime DateValue { get; set; }
            [DataMember(Order = 4)]
            public string StringValue { get; set; }
        }
    }
}
