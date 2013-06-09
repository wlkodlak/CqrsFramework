using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Serialization;
using System.Reflection;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class MessageTypeResolverTest
    {
        private MessageTypeResolver _resolver;

        [TestMethod]
        public void RegisterMultipleTypes()
        {
            PrepareResolver();
        }

        [TestMethod]
        public void GetTypeByName()
        {
            PrepareResolver();
            Assert.AreEqual(typeof(string), _resolver.GetType("string"));
            Assert.AreEqual(typeof(Message1), _resolver.GetType("Message1Typename"));
            Assert.AreEqual(typeof(Message2), _resolver.GetType("Message2Name"));
            Assert.AreEqual(typeof(Message3), _resolver.GetType("EventMessage3"));
        }

        [TestMethod]
        public void GetNameByType()
        {
            PrepareResolver();
            Assert.AreEqual("string", _resolver.GetName(typeof(string)));
            Assert.AreEqual("Message1Typename", _resolver.GetName(typeof(Message1)));
            Assert.AreEqual("Message2Name", _resolver.GetName(typeof(Message2)));
            Assert.AreEqual("EventMessage3", _resolver.GetName(typeof(Message3)));
        }

        [TestMethod]
        public void GetTagsOfType()
        {
            PrepareResolver();
            AssertExtension.AreEqual(new string[] { "string" }, _resolver.GetTags("string"));
            AssertExtension.AreEqual(new string[] { "Command", "Message1Typename", "Service1Command" }, _resolver.GetTags("Message1Typename"));
            AssertExtension.AreEqual(new string[] { "Command", "Message2Name", "Service2Command" }, _resolver.GetTags("Message2Name"));
            AssertExtension.AreEqual(new string[] { "Event", "EventMessage3" }, _resolver.GetTags("EventMessage3"));
        }

        [TestMethod]
        public void GetTypesWithTag()
        {
            PrepareResolver();
            AssertExtension.AreEqual(new string[] { "EventMessage3" }, _resolver.GetTypes("Event"));
            AssertExtension.AreEqual(new string[] { "Message2Name" }, _resolver.GetTypes("Service2Command"));
            AssertExtension.AreEqual(new string[] { "Message1Typename", "Message2Name" }, _resolver.GetTypes("Command"));
        }

        [TestMethod]
        public void UnknownTypes()
        {
            PrepareResolver();
            Assert.IsNull(_resolver.GetType("undefined"), "GetType");
            Assert.IsNull(_resolver.GetName(typeof(MessageTypeResolverTest)), "GetName");
            CollectionAssert.AreEqual(new string[0], _resolver.GetTags("undefined"), "GetTags");
            CollectionAssert.AreEqual(new string[0], _resolver.GetTypes("tag"), "GetTypes");
        }

        private void PrepareResolver()
        {
            _resolver = new MessageTypeResolver();
            _resolver.RegisterType(typeof(string), "string");
            _resolver.RegisterType(typeof(Message1), "Message1Typename", "Command", "Service1Command");
            _resolver.RegisterType(typeof(Message2), "Message2Name", "Service2Command", "Command");
            _resolver.RegisterType(typeof(Message3), "EventMessage3", "Event");
        }

        private class Message1 : ICommand
        {
        }

        private class Message2 : ICommand
        {
        }

        private class Message3 : IEvent
        {
        }
    }
}
