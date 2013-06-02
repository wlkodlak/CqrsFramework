using System;
using System.Collections.Generic;
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

        [TestInitialize]
        public void Initialize()
        {
            _time = new Mock<ITimeProvider>();
            _time.Setup(t => t.Get()).Returns(() => _now);
        }

        [TestMethod]
        public void OnlyDefaultWithRetryAndDrop()
        {
            var policy = new MessageErrorPolicy(_time.Object);
            policy.Default.Retry(5).Delay(100, 50);
            TestRetry(policy, 0, new Exception(), 150);
            TestRetry(policy, 1, new Exception(), 200);
            TestRetry(policy, 4, new Exception(), 350);
            TestDrop(policy, 5, new Exception());
        }

        [TestMethod]
        public void OnlyDefaultWithErrorQueue()
        {
            var policy = new MessageErrorPolicy(_time.Object);
            var error = CreateErrorQueue();
            policy.Default.Retry(5).Delay(-100, 100, 10, 0, 1).ErrorQueue(error.Object);
            TestRetry(policy, 0, new Exception(), 11);
            TestRetry(policy, 1, new Exception(), 156);
            TestRetry(policy, 4, new Exception(), 1275);
            TestRedirect(policy, 5, new Exception(), error);
        }

        [TestMethod]
        public void VariousTypes()
        {
            var policy = new MessageErrorPolicy(_time.Object);
            var error = CreateErrorQueue();
            policy.Default.Retry(5).Delay(100, 50);
            policy.For<ArgumentException>().Retry(3).Delay(-100, 100, 10, 0, 1).ErrorQueue(error.Object);
            policy.For<InvalidOperationException>().Retry(0).ErrorQueue(error.Object);
            policy.For<ArgumentNullException>().Retry(1).Delay(100).Drop();
            TestRetry(policy, 4, new KeyNotFoundException(), 350);
            TestRetry(policy, 1, new ArgumentOutOfRangeException(), 156);
            TestDrop(policy, 1, new ArgumentNullException(), error);
            TestRedirect(policy, 0, new InvalidOperationException(), error);
        }

        private Mock<IMessageInboxWriter> CreateErrorQueue()
        {
            var mock = new Mock<IMessageInboxWriter>(MockBehavior.Strict);
            mock.Setup(w => w.Put(It.IsAny<Message>()));
            return mock;
        }

        private string Comment(string message, int retry, Exception exception)
        {
            return string.Format("Attempt {0} at {1}: {2}", retry, exception.GetType().Name, message);
        }

        private void TestRetry(IMessageErrorPolicy policy, int retry, Exception exception, int delay)
        {
            var message = new Message("Hello world");
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-2);
            if (retry > 0)
                message.Headers.RetryNumber = retry;
            var inbox = new Mock<IMessageInboxReader>(MockBehavior.Strict);
            inbox.Setup(i => i.Put(message));
            policy.HandleException(inbox.Object, message, exception);
            Assert.AreEqual(retry + 1, message.Headers.RetryNumber, Comment("Retry number", retry, exception));
            var realDelay = (int)(message.Headers.DeliverOn - _now).TotalMilliseconds;
            Assert.AreEqual(delay, realDelay, Comment("Delay", retry, exception));
            inbox.Verify(i => i.Put(message), Times.Once(), Comment("Put", retry, exception));
        }

        private void TestDrop(IMessageErrorPolicy policy, int retry, Exception exception, params Mock<IMessageInboxWriter>[] errors)
        {
            var message = new Message("Hello world");
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-2);
            if (retry > 0)
                message.Headers.RetryNumber = retry;
            var inbox = new Mock<IMessageInboxReader>(MockBehavior.Strict);
            inbox.Setup(i => i.Delete(message));
            policy.HandleException(inbox.Object, message, exception);
            inbox.Verify(i => i.Delete(message), Times.Once(), Comment("Delete", retry, exception));
            foreach (var error in errors)
                error.Verify(i => i.Put(message), Times.Never(), Comment("Error", retry, exception));
        }

        private void TestRedirect(IMessageErrorPolicy policy, int retry, Exception exception, Mock<IMessageInboxWriter> error)
        {
            var message = new Message("Hello world");
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-2);
            if (retry > 0)
                message.Headers.RetryNumber = retry;
            var inbox = new Mock<IMessageInboxReader>(MockBehavior.Strict);
            inbox.Setup(i => i.Delete(message)).Verifiable();
            policy.HandleException(inbox.Object, message, exception);
            inbox.Verify(i => i.Delete(message), Times.Once(), Comment("Delete inbox", retry, exception));
            error.Verify(e => e.Put(message), Times.Once(), Comment("Error", retry, exception));
        }

    }
}
