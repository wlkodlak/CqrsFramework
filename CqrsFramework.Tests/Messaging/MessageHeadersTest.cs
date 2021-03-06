﻿using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Messaging;

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
            var now = new DateTime(2013, 6, 1, 14, 0, 0);

            var headers = new MessageHeaders();
            headers.MessageId = msgId;
            headers.CorellationId = crlId;
            headers.CreatedOn = now;
            headers.DeliverOn = now.AddSeconds(120);
            headers.ValidUntil = now.AddSeconds(600);
            headers.RetryNumber = 2;
            headers.ResourcePath = "topic/49304";
            headers.TypePath = "Event.Supertype.Subtype";
            headers.PayloadFormat = "json";
            headers.PayloadLength = 218;
            headers.PayloadType = "Subtype";
            headers.EventClock = 5472;

            Assert.AreEqual(msgId.ToString("D"), headers["MessageId"]);
            Assert.AreEqual("2", headers["RetryNumber"]);
            Assert.AreEqual("2013-06-01 14:02:00.0000", headers["DeliverOn"]);
            Assert.AreEqual("2013-06-01 14:10:00.0000", headers["ValidUntil"]);
            Assert.AreEqual("218", headers["PayloadLength"]);
            Assert.AreEqual("Subtype", headers["PayloadType"]);
            Assert.AreEqual("5472", headers["EventClock"]);
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
            headers.DeliverOn = now.AddSeconds(120);
            headers.ResourcePath = "topic/49304";
            headers["CustomHeader"] = "CustomValue";

            var enumerated = headers.ToDictionary(h => h.Name);
            Assert.AreEqual(6, enumerated.Count, "Headers count");
            foreach (var name in new string[] { "MessageId", "CreatedOn", "DeliverOn", "ResourcePath" })
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
