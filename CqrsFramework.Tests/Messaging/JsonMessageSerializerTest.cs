using System;
using System.Collections.Generic;
using System.Linq;
using CqrsFramework.ServiceBus;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Runtime.Serialization;
using System.IO;
using System.Text;
using KellermanSoftware.CompareNetObjects;
using ServiceStack.Text;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class JsonMessageSerializerTest
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
            _serializer = new JsonMessageSerializer(knownTypes);

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
                    .Header("Typename", "TestMessage1")
                    .Header("MessageId","4d2274aa-9423-abed-d8a3-8712e36bb14c")
                    .Header("CreatedOn","2013-06-01 18:20:00.0000")
                    .Header("DeliverOn","2013-06-01 18:22:30.0000");
                header.WriteTo(stream);

                JsonSerializer.SerializeToStream(_contents1, stream);
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
                    .Header("Typename", "TestMessage2")
                    .Header("MessageId","8452cdd3-d844-3214-bb8a-1b0ed84572d2")
                    .Header("CorellationId","4d2274aa-9423-abed-d8a3-8712e36bb14c")
                    .Header("CreatedOn","2013-06-01 18:22:45.0000");
                header.WriteTo(stream);
                JsonSerializer.SerializeToStream(_contents2, stream);

                var deserialized = _serializer.Deserialize(stream.ToArray());
                AssertEqualsDeep(_message2, deserialized);
            }
        }

        private class TestWriter
        {
            private MemoryStream _stream;
            private StreamWriter _writer;
            public TestWriter()
            {
                _stream = new MemoryStream();
                _writer = new StreamWriter(_stream, new UTF8Encoding(false), 1024, true);
            }
            public TestWriter Header(string name, string value)
            {
                _writer.WriteLine("{0}: {1}", name, value);
                return this;
            }
            public void WriteTo(Stream stream)
            {
                _writer.WriteLine();
                _writer.Dispose();
                _stream.WriteTo(stream);
            }
            public byte[] ToArray()
            {
                _writer.WriteLine();
                _writer.Dispose();
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
            var utf8 = new UTF8Encoding(false);
            var expectedString = utf8.GetString(expected);
            var actualString = utf8.GetString(actual);
            Assert.AreEqual(expectedString, actualString);
        }
    }
}
