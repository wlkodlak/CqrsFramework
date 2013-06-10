﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Moq;
using System.IO;
using CqrsFramework.Serialization;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class MessageSerializerTextTest
    {
        private Mock<IMessageBodySerializer> _bodySerializer;
        private IMessageSerializer _serializer;
        private Guid _guid1, _guid2;
        private Message _message1, _message2;
        private TestMessage1 _contents1;
        private TestMessage2 _contents2;
        private byte[] _serialized1, _serialized2;

        [TestInitialize]
        public void Initialize()
        {
            _bodySerializer = new Mock<IMessageBodySerializer>();
            _bodySerializer
                .Setup(s => s.Serialize(It.IsAny<object>(), It.IsAny<MessageHeaders>()))
                .Returns<object, MessageHeaders>(SerializeBody);
            _bodySerializer
                .Setup(s => s.Deserialize(It.IsAny<byte[]>(), It.IsAny<MessageHeaders>()))
                .Returns<byte[], MessageHeaders>(DeserializeBody);

            _serializer = new TextMessageSerializer(_bodySerializer.Object);

            _guid1 = new Guid("4d2274aa-9423-abed-d8a3-8712e36bb14c");
            _contents1 = new TestMessage1() { IntValue = 44, StringValue = "Hello" };
            _message1 = new Message(_contents1);
            _message1.Headers.MessageId = _guid1;
            _message1.Headers.CreatedOn = new DateTime(2013, 6, 1, 18, 20, 00);
            _message1.Headers.DeliverOn = new DateTime(2013, 6, 1, 18, 22, 30);
            _serialized1 = SerializeBody(_contents1, new MessageHeaders());

            _guid2 = new Guid("8452cdd3-d844-3214-bb8a-1b0ed84572d2");
            _contents2 = new TestMessage2() { DateValue = new DateTime(2013, 8, 5), StringValue = "White Rabbit" };
            _message2 = new Message(_contents2);
            _message2.Headers.MessageId = _guid2;
            _message2.Headers.CorellationId = _guid1;
            _message2.Headers.CreatedOn = new DateTime(2013, 6, 1, 18, 22, 45);
            _serialized2 = SerializeBody(_contents2, new MessageHeaders());
        }

        private byte[] SerializeBody(object payload, MessageHeaders headers)
        {
            using (var stream = new MemoryStream())
            {
                var type = payload.GetType();
                var dcs = new DataContractSerializer(type);
                dcs.WriteObject(stream, payload);
                var bytes = stream.ToArray();

                headers.PayloadLength = bytes.Length;
                headers.PayloadType = type.Name;

                return bytes;
            }
        }

        private object DeserializeBody(byte[] bytes, MessageHeaders headers)
        {
            using (var stream = new MemoryStream(bytes))
            {
                var type = headers.PayloadType == "TestMessage1" ? typeof(TestMessage1) : typeof(TestMessage2);
                headers.PayloadLength = 0;
                headers.PayloadType = null;
                var dcs = new DataContractSerializer(type);
                return dcs.ReadObject(stream);
            }
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

        [TestMethod]
        public void RoundTrip1()
        {
            var serialized = _serializer.Serialize(_message1);
            var deserialized = _serializer.Deserialize(serialized);
            AssertExtension.AreEqual(_message1, deserialized);
        }

        [TestMethod]
        public void RoundTrip2()
        {
            var serialized = _serializer.Serialize(_message2);
            var deserialized = _serializer.Deserialize(serialized);
            AssertExtension.AreEqual(_message2, deserialized);
        }

        [TestMethod]
        public void Serialize()
        {
            var expected = new TestWriter()
                .Header("MessageId", "4d2274aa-9423-abed-d8a3-8712e36bb14c")
                .Header("CreatedOn", "2013-06-01 18:20:00.0000")
                .Header("DeliverOn", "2013-06-01 18:22:30.0000")
                .Header("PayloadLength", _serialized1.Length.ToString())
                .Header("PayloadType", "TestMessage1");
            expected.Body(_serialized1);

            var serialized = _serializer.Serialize(_message1);
            AssertExtension.AreEqual(expected.ToArray(), serialized);
        }

        [TestMethod]
        public void Deserialize()
        {
            var expected = new TestWriter()
                .Header("MessageId", "8452cdd3-d844-3214-bb8a-1b0ed84572d2")
                .Header("CorellationId", "4d2274aa-9423-abed-d8a3-8712e36bb14c")
                .Header("CreatedOn", "2013-06-01 18:22:45.0000")
                .Header("PayloadLength", _serialized2.Length.ToString())
                .Header("PayloadType", "TestMessage2");
            expected.Body(_serialized2);

            var deserialized = _serializer.Deserialize(expected.ToArray());
            AssertExtension.AreEqual(_message2, deserialized);
        }

        private class TestWriter
        {
            StringBuilder _headers = new StringBuilder();
            byte[] _body;

            public TestWriter Header(string name, string value)
            {
                _headers.AppendLine(string.Format("{0}: {1}", name, value));
                return this;
            }

            public TestWriter Body(byte[] payload)
            {
                _body = payload;
                return this;
            }

            public byte[] ToArray()
            {
                var headers = new UTF8Encoding(false).GetBytes(_headers.ToString() + Environment.NewLine);
                return headers.Concat(_body ?? new byte[0]).ToArray();
            }
        }
    }
}
