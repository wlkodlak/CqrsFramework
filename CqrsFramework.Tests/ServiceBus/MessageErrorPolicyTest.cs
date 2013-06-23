using System;
using System.Collections.Generic;
using Moq;
using CqrsFramework.ServiceBus;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.ServiceBus
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
            TestRedirect(policy, 5, new Exception(), error.Object);
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
            TestDrop(policy, 1, new ArgumentNullException());
            TestRedirect(policy, 0, new InvalidOperationException(), error.Object);
        }

        private Mock<IMessageInboxWriter> CreateErrorQueue()
        {
            return new Mock<IMessageInboxWriter>(MockBehavior.Strict);
        }

        private string Comment(string message, int retry, Exception exception)
        {
            return string.Format("Attempt {0} at {1}: {2}", retry, exception.GetType().Name, message);
        }

        private void TestRetry(IMessageErrorPolicy policy, int retry, Exception exception, int delay)
        {
            var action = policy.HandleException(retry, exception);
            var expected = MessageErrorAction.Retry(TimeSpan.FromMilliseconds(delay));
            Assert.AreEqual(expected, action, Comment("Action", retry, exception));
        }

        private void TestDrop(IMessageErrorPolicy policy, int retry, Exception exception)
        {
            var action = policy.HandleException(retry, exception);
            var expected = MessageErrorAction.Drop();
            Assert.AreEqual(expected, action, Comment("Action", retry, exception));
        }

        private void TestRedirect(IMessageErrorPolicy policy, int retry, Exception exception, IMessageInboxWriter error)
        {
            var action = policy.HandleException(retry, exception);
            var expected = MessageErrorAction.Redirect(error);
            Assert.AreEqual(expected, action, Comment("Action", retry, exception));
        }

    }
}
