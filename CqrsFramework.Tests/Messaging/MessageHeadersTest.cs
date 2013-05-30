using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class MessageHeadersTest
    {
        [TestMethod]
        public void DictionaryValues()
        {
            var headers = new MessageHeaders();
            headers["Hello"] = "World";
            headers["Message"] = "Id";
            Assert.AreEqual("World", headers["Hello"]);
            Assert.AreEqual("Id", headers["Message"]);
            Assert.IsNull(headers["Nondefined"]);
        }

        [TestMethod]
        public void NamedValues()
        {
            var msgId = Guid.NewGuid();
            var crlId = Guid.NewGuid();
            var now = DateTime.UtcNow;

            var headers = new MessageHeaders();
            headers.MessageId = msgId;
            headers.CorellationId = crlId;
            headers.CreatedOn = now;
            headers.Delay = TimeSpan.FromSeconds(120);
            headers.TimeToLive = TimeSpan.FromSeconds(600);
            headers.RetryNumber = 2;
            headers.ResourcePath = "topic/49304";
            headers.TypePath = "Event.Supertype.Subtype";

            Assert.AreEqual(msgId.ToString("D"), headers["MessageId"]);
            Assert.AreEqual("2", headers["RetryNumber"]);
            Assert.AreEqual("600", headers["TimeToLive"]);
        }

        [TestMethod]
        public void Enumerate()
        {
            var msgId = Guid.NewGuid();
            var crlId = Guid.NewGuid();
            var now = DateTime.UtcNow;

            var headers = new MessageHeaders();
            headers.MessageId = msgId;
            headers.CorellationId = crlId;
            headers.CreatedOn = now;
            headers.Delay = TimeSpan.FromSeconds(120);
            headers.ResourcePath = "topic/49304";
            headers["CustomHeader"] = "CustomValue";

            var enumerated = headers.ToDictionary(h => h.Name);
            Assert.AreEqual(6, enumerated.Count, "Headers count");
            foreach (var name in new string[] { "MessageId", "CreatedOn", "Delay", "ResourcePath" })
            {
                Assert.AreEqual(headers[name], enumerated[name].Value, "Value of {0}", name);
                Assert.IsFalse(enumerated[name].CopyToEvent, "Copy to event of {0}", name);
            }
            foreach (var name in new string[] { "CorellationId", "CustomHeader" })
            {
                Assert.AreEqual(headers[name], enumerated[name].Value, "Value of {0}", name);
                Assert.IsTrue(enumerated[name].CopyToEvent, "Copy to event of {0}", name);
            }
        }
    }
}
