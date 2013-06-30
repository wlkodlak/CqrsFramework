using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;
using Moq;
using System.Xml.Linq;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class KeyValueProjectionStrategyTest
    {
        private Mock<IMessageBodySerializer> _serializer;

        [TestInitialize]
        public void Initialize()
        {
            _serializer = new Mock<IMessageBodySerializer>(MockBehavior.Strict);
        }

        [TestMethod]
        public void BinaryClock()
        {
            var strategy = new KeyValueProjectionStrategy(_serializer.Object, "TypeName", KeyValueProjectionClockFormat.Binary);
            AssertExtension.AreEqual(ByteArrayUtils.BinaryLong(5842215467524), strategy.SerializeClock(5842215467524));
            Assert.AreEqual(584221546654567424, strategy.DeserializeClock(ByteArrayUtils.BinaryLong(584221546654567424)));
        }

        [TestMethod]
        public void HexClock()
        {
            var strategy = new KeyValueProjectionStrategy(_serializer.Object, "TypeName", KeyValueProjectionClockFormat.Hex);
            AssertExtension.AreEqual(ByteArrayUtils.HexLong(5842215467524), strategy.SerializeClock(5842215467524));
            Assert.AreEqual(584221546654567424, strategy.DeserializeClock(ByteArrayUtils.HexLong(584221546654567424)));
        }

        [TestMethod]
        public void TextClock()
        {
            var strategy = new KeyValueProjectionStrategy(_serializer.Object, "TypeName", KeyValueProjectionClockFormat.Text);
            AssertExtension.AreEqual(ByteArrayUtils.TextLong(5842215467524), strategy.SerializeClock(5842215467524));
            Assert.AreEqual(584221546654567424, strategy.DeserializeClock(ByteArrayUtils.TextLong(584221546654567424)));
        }

        [TestMethod]
        public void GetTypename()
        {
            var strategy = new KeyValueProjectionStrategy(_serializer.Object, "TypeName", KeyValueProjectionClockFormat.Binary);
            Assert.AreEqual("TypeName", strategy.GetTypename(typeof(string)));
        }

        [TestMethod]
        public void SerializeView()
        {
            var view = new XElement("Elem", "Text");
            var bytes = ByteArrayUtils.Utf8Text(view.ToString());
            _serializer.Setup(s => s.Serialize(view, It.IsAny<MessageHeaders>()))
                .Returns<object, MessageHeaders>((o, h) =>
                {
                    Assert.IsNotNull(h);
                    h.PayloadType = "TypeName";
                    return bytes;
                }).Verifiable();
            _serializer.Setup(s => s.Deserialize(bytes, It.IsAny<MessageHeaders>()))
                .Returns<byte[], MessageHeaders>((b, h) =>
                {
                    Assert.IsNotNull(h);
                    Assert.AreEqual("TypeName", h.PayloadType);
                    return XElement.Parse(ByteArrayUtils.Utf8Text(b));
                }).Verifiable();
            var strategy = new KeyValueProjectionStrategy(_serializer.Object, "TypeName", KeyValueProjectionClockFormat.Binary);
            AssertExtension.AreEqual(bytes, strategy.SerializeView(typeof(XElement), view));
            AssertExtension.AreEqual(view, strategy.DeserializeView(typeof(XElement), bytes));
        }
    }
}
