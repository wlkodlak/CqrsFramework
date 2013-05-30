using System;
using CqrsFramework;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    public interface IMessageInboxTestBuilder
    {
        public void AddMessage(Message message);
        public IMessageInbox BuildInbox();
    }

    public abstract class MessageInboxTestBase
    {
        protected abstract IMessageInboxTestBuilder CreateInbox();

        [TestMethod]
        public void CanPutMessageToInbox()
        {
            var inbox = CreateInbox();
            var message = new Message("Hello world");
            inbox.Put(message);
        }
    }
}
