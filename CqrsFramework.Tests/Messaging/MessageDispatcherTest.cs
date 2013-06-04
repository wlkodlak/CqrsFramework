using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.ServiceBus;
using System.Xml.Linq;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class MessageDispatcherTest
    {
        private MessageDispatcher _dispatcher;
        private SimpleRegistrationClass _handler1, _handler2;
        private Message _message1, _message2;

        private class SimpleRegistrationClass
        {
            public List<string> CallsString = new List<string>();
            public List<Tuple<XElement, MessageHeaders>> CallsElement = new List<Tuple<XElement, MessageHeaders>>();

            public void Handle(string param)
            {
                CallsString.Add(param);
            }

            public void When(XElement elem, MessageHeaders headers)
            {
                CallsElement.Add(new Tuple<XElement, MessageHeaders>(elem, headers));
            }

        }

        [TestInitialize]
        public void Initialize()
        {
            _dispatcher = new MessageDispatcher();
            _handler1 = new SimpleRegistrationClass();
            _handler2 = new SimpleRegistrationClass();
            _message1 = new Message("Hello world");
            _message2 = new Message(new XElement("Root", "Hello"));
            _message2.Headers.RetryNumber = 3;
        }

        [TestMethod]
        public void CallsRegistrator()
        {
            var mock = new Mock<IMessageDispatcherRegistrator>();
            mock.Setup(r => r.RegisterToDispatcher(_dispatcher)).Verifiable();
            _dispatcher.UseRegistrator(mock.Object);
            mock.Verify();
        }

        [TestMethod]
        public void RegisterShort()
        {
            _dispatcher.Register<string>(_handler1.Handle);
            _dispatcher.Dispatch(_message1);
            Assert.AreEqual(_message1.Payload, _handler1.CallsString.Single());
        }

        [TestMethod]
        public void RegisterLong()
        {
            _dispatcher.Register<XElement>(_handler1.When);
            _dispatcher.Dispatch(_message2);
            var call = _handler1.CallsElement.Single();
            Assert.AreEqual(_message2.Payload, call.Item1);
            Assert.AreEqual(3, call.Item2.RetryNumber);
        }

        [TestMethod]
        public void UnknownHandler()
        {
            try
            {
                _dispatcher.Register<string>(_handler1.Handle);
                _dispatcher.Register<XElement>(_handler1.When);
                _dispatcher.Dispatch(new Message(5));
                Assert.Fail("Expected MessageDispatcherException");
            }
            catch (MessageDispatcherException)
            {
            }
        }

        [TestMethod]
        public void AutoRegister()
        {
            _dispatcher.AutoRegister(_handler1);
            _dispatcher.Dispatch(_message2);
            var call = _handler1.CallsElement.Single();
            Assert.AreEqual(_message2.Payload, call.Item1);
            Assert.AreEqual(3, call.Item2.RetryNumber);
        }
    }
}
