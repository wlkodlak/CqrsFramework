using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CqrsFramework.Messaging;
using CqrsFramework.EventStore;
using System.Threading.Tasks;
using Moq;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class ProjectionDispatcherTest
    {
        private Mock<IRebuildableProjection> redirectProjection;
        private TestProjection testProjection;

        private class TestProjection : IRebuildableProjection
        {
            public List<string> StringEvents = new List<string>();
            public List<XElement> ElementEvents = new List<XElement>();

            public void BeginUpdate()
            {
            }

            public void EndUpdate()
            {
            }

            public void Reset()
            {
                StringEvents.Clear();
                ElementEvents.Clear();
            }

            public bool NeedsRebuild()
            {
                return true;
            }

            public long GetClockToHandle()
            {
                return 0;
            }

            public void When(string s)
            {
                StringEvents.Add(s);
            }

            public void Handle(XElement elem, MessageHeaders hdr)
            {
                ElementEvents.Add(new XElement("Event", new XAttribute("MessageId", hdr.MessageId), elem));
            }

            public void Handle(IXmlSerializable elem, MessageHeaders hdr)
            {
                ElementEvents.Add(new XElement("Event", new XAttribute("MessageId", hdr.MessageId), elem));
            }
        }

        [TestInitialize]
        public void Initialize()
        {
            redirectProjection = new Mock<IRebuildableProjection>(MockBehavior.Strict);
            testProjection = new TestProjection();
        }

        [TestMethod]
        public void RegisterProjection()
        {
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
        }

        [TestMethod]
        public void RedirectsBeginUpdate()
        {
            redirectProjection.Setup(s => s.BeginUpdate()).Verifiable();
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
            dispatcher.BeginUpdate();
            redirectProjection.Verify();
        }

        [TestMethod]
        public void RedirectsEndUpdate()
        {
            redirectProjection.Setup(s => s.EndUpdate()).Verifiable();
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
            dispatcher.EndUpdate();
            redirectProjection.Verify();
        }

        [TestMethod]
        public void RedirectsGetClockToHandle()
        {
            redirectProjection.Setup(s => s.GetClockToHandle()).Returns(4).Verifiable();
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
            Assert.AreEqual(4, dispatcher.GetClockToHandle());
            redirectProjection.Verify();
        }

        [TestMethod]
        public void RedirectsNeedsRebuild()
        {
            redirectProjection.Setup(s => s.NeedsRebuild()).Returns(true).Verifiable();
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
            Assert.IsTrue(dispatcher.NeedsRebuild());
            redirectProjection.Verify();
        }

        [TestMethod]
        public void RedirectsReset()
        {
            redirectProjection.Setup(s => s.Reset()).Verifiable();
            var dispatcher = new ProjectionDispatcher(redirectProjection.Object);
            dispatcher.Reset();
            redirectProjection.Verify();
        }

        [TestMethod]
        public void RegisterString()
        {
            var dispatcher = new ProjectionDispatcher(testProjection);
            dispatcher.Register<string>(testProjection.When);
            var message = new Message("Hello world");
            dispatcher.Dispatch(message);
            Assert.AreEqual(1, testProjection.StringEvents.Count, "Events count");
            Assert.AreEqual("Hello world", testProjection.StringEvents[0], "Contents");
        }

        [TestMethod]
        public void RegisterXElement()
        {
            var dispatcher = new ProjectionDispatcher(testProjection);
            dispatcher.Register<XElement>(testProjection.Handle);
            var element = new XElement("TestEvent", "Test contents");
            var messageId = Guid.NewGuid();
            var expected = new XElement("Event", new XAttribute("MessageId", messageId.ToString("D")), element);
            var message = new Message(element);
            message.Headers.MessageId = messageId;
            dispatcher.Dispatch(message);
            Assert.AreEqual(1, testProjection.ElementEvents.Count);
            AssertExtension.AreEqual(expected, testProjection.ElementEvents[0], "Contents");
        }

        [TestMethod]
        public void RegisterInterface()
        {
            var dispatcher = new ProjectionDispatcher(testProjection);
            dispatcher.Register<IXmlSerializable>(testProjection.Handle);
            var element = new XElement("TestEvent", "Test contents");
            var messageId = Guid.NewGuid();
            var expected = new XElement("Event", new XAttribute("MessageId", messageId.ToString("D")), element);
            var message = new Message(element);
            message.Headers.MessageId = messageId;
            dispatcher.Dispatch(message);
            Assert.AreEqual(1, testProjection.ElementEvents.Count);
            AssertExtension.AreEqual(expected, testProjection.ElementEvents[0], "Contents");
        }

        [TestMethod]
        public void HandlerNotFound()
        {
            var dispatcher = new ProjectionDispatcher(testProjection);
            dispatcher.Register<string>(testProjection.When);
            dispatcher.Register<XElement>(testProjection.Handle);
            dispatcher.Dispatch(new Message(5847));
            Assert.AreEqual(0, testProjection.ElementEvents.Count, "xelement");
            Assert.AreEqual(0, testProjection.StringEvents.Count, "string");
        }

        [TestMethod]
        public void CannotRegisterToAnotherObject()
        {
            try
            {
                var another = new TestProjection();
                var dispatcher = new ProjectionDispatcher(testProjection);
                dispatcher.Register<string>(another.When);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        [TestMethod]
        public void AutoRegister()
        {
            var dispatcher = new ProjectionDispatcher(testProjection);
            dispatcher.AutoRegister();
            var element = new XElement("TestEvent", "Test contents");
            var messageId = Guid.NewGuid();
            var expected = new XElement("Event", new XAttribute("MessageId", messageId.ToString("D")), element);
            var message = new Message(element);
            message.Headers.MessageId = messageId;
            dispatcher.Dispatch(message);
            Assert.AreEqual(1, testProjection.ElementEvents.Count);
            AssertExtension.AreEqual(expected, testProjection.ElementEvents[0], "Contents");
        }
    }
}
