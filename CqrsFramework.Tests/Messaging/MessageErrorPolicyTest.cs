using System;
using Moq;
using CqrsFramework.ServiceBus;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class MessageErrorPolicyTest
    {
        private DateTime _now = new DateTime(2013, 6, 2, 18, 0, 0);
        private Mock<ITimeProvider> _time;
        private Mock<IMessageInboxReader> _inbox;

        [TestInitialize]
        public void Initialize()
        {
            _time = new Mock<ITimeProvider>();
            _time.Setup(t => t.Get()).Returns(() => _now);
            _inbox = new Mock<IMessageInboxReader>(MockBehavior.Strict);
        }

        [TestMethod]
        public void SetupDefault()
        {
            var policy = new MessageErrorPolicy();
            policy.Default.Retry(5, 100, 50);
            TestRetry(policy, 0, new Exception(), 150);
        }

        private void TestRetry(IMessageErrorPolicy policy, int retry, Exception exception, int delay)
        {
            var message = new Message("Hello world");
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-2);
            policy.HandleException(_inbox.Object, message, exception);
        }
    }
}
