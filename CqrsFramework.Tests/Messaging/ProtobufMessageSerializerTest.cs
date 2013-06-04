using System;
using System.Collections.Generic;
using System.Linq;
using CqrsFramework.ServiceBus;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProtoBuf;
using System.Runtime.Serialization;
using System.IO;
using System.Text;
using KellermanSoftware.CompareNetObjects;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class ProtobufMessageSerializerTest
    {
        private IMessageSerializer _serializer;
        private Guid _guid1, _guid2;
        private Message _message1, _message2;
        private TestMessage1 _contents1;
        private TestMessage2 _contents2;

        [TestInitialize]
        public void Initialize()
        {
            var knownTypes = new Type[] { typeof(TestMessage1), typeof(TestMessage2) };
            _serializer = new ProtobufMessageSerializer(knownTypes);

            _guid1 = new Guid("4d2274aa-9423-abed-d8a3-8712e36bb14c");
            _contents1 = new TestMessage1() { IntValue = 44, StringValue = "Hello" };
            _message1 = new Message(_contents1);
            _message1.Headers.MessageId = _guid1;
            _message1.Headers.CreatedOn = new DateTime(2013, 6, 1, 18, 20, 00);
            _message1.Headers.DeliverOn = new DateTime(2013, 6, 1, 18, 22, 30);

            _guid2 = new Guid("8452cdd3-d844-3214-bb8a-1b0ed84572d2");
            _contents2 = new TestMessage2() { DateValue = new DateTime(2013, 8, 5), StringValue = "White Rabbit" };
            _message2 = new Message(_contents2);
            _message2.Headers.MessageId = _guid2;
            _message2.Headers.CorellationId = _guid1;
            _message2.Headers.CreatedOn = new DateTime(2013, 6, 1, 18, 22, 45);
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

        [TestMethod]
        public void RoundTrip1()
        {
            var serialized = _serializer.Serialize(_message1);
            var deserialized = _serializer.Deserialize(serialized);
            AssertEqualsDeep(_message1, deserialized);
        }

        [TestMethod]
        public void RoundTrip2()
        {
            var serialized = _serializer.Serialize(_message2);
            var deserialized = _serializer.Deserialize(serialized);
            AssertEqualsDeep(_message2, deserialized);
        }

        [TestMethod]
        public void Serialize()
        {
            using (var stream = new MemoryStream())
            {
                var now = Guid.NewGuid();
                var header = new TestWriter()
                    .ShortString("TestMessage1").Byte(3)
                    .ShortString("MessageId").ShortString("4d2274aa-9423-abed-d8a3-8712e36bb14c")
                    .ShortString("CreatedOn").ShortString("2013-06-01 18:20:00.0000")
                    .ShortString("DeliverOn").ShortString("2013-06-01 18:22:30.0000");
                header.WriteTo(stream);
                Serializer.Serialize<TestMessage1>(stream, _contents1);
                var expected = stream.ToArray();
                var serialized = _serializer.Serialize(_message1);
                AssertEqualsBytes(expected, serialized);
            }
        }

        [TestMethod]
        public void Deserialize()
        {
            using (var stream = new MemoryStream())
            {
                var header = new TestWriter()
                    .ShortString("TestMessage2").Byte(3)
                    .ShortString("MessageId").ShortString("8452cdd3-d844-3214-bb8a-1b0ed84572d2")
                    .ShortString("CorellationId").ShortString("4d2274aa-9423-abed-d8a3-8712e36bb14c")
                    .ShortString("CreatedOn").ShortString("2013-06-01 18:22:45.0000");
                header.WriteTo(stream);
                Serializer.Serialize<TestMessage2>(stream, _contents2);

                var deserialized = _serializer.Deserialize(stream.ToArray());
                AssertEqualsDeep(_message2, deserialized);
            }
        }

        private class TestWriter
        {
            private MemoryStream _stream;
            public TestWriter()
            {
                _stream = new MemoryStream();
            }
            public TestWriter Byte(byte b)
            {
                _stream.WriteByte(b);
                return this;
            }
            public TestWriter ShortString(string s)
            {
                _stream.WriteByte((byte)(s.Length));
                var buffer = Encoding.ASCII.GetBytes(s);
                _stream.Write(buffer, 0, buffer.Length);
                return this;
            }
            public void WriteTo(Stream stream)
            {
                _stream.WriteTo(stream);
            }
            public byte[] ToArray()
            {
                return _stream.ToArray();
            }
        }

        private void AssertEqualsDeep(Message expected, Message actual)
        {
            var comparer = new CompareObjects();
            comparer.MaxDifferences = 3;

            var expectedHeaders = expected.Headers.ToList();
            var actualHeaders = actual.Headers.ToList();
            if (!comparer.Compare(expectedHeaders, actualHeaders))
                throw new Exception(comparer.DifferencesString);
            
            if (!comparer.Compare(expected.Payload, actual.Payload))
                throw new Exception(comparer.DifferencesString);
        }

        private void AssertEqualsBytes(byte[] expected, byte[] actual)
        {
            var min = Math.Min(expected.Length, actual.Length);
            for (int i = 0; i < min; i++)
                Assert.AreEqual(expected[i], actual[i], "Difference at {0}", i);
            Assert.AreEqual(expected.Length, actual.Length, "Different lengths");
        }
    }
}
