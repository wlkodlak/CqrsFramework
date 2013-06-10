using System;
using System.Linq;
using System.Collections.Generic;
using Moq;
using CqrsFramework;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.Messaging
{
    public interface IMessageInboxTestBuilder : IDisposable
    {
        IMessageInboxWriter GetWriter();
        IMessageInboxReader GetInbox();
    }

    public abstract class MessageInboxTestBase
    {
        protected abstract IMessageInboxTestBuilder CreateBuilder();

        [TestMethod]
        [Timeout(1000)]
        public void PutThenReceive()
        {
            using (var builder = CreateBuilder())
            {
                var message = new Message("Hello world");
                var writer = builder.GetWriter();
                var inbox = builder.GetInbox();
                writer.Put(message);
                var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
                inbox.Delete(received);
                Assert.AreEqual(message, received);
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveThenPut()
        {
            using (var builder = CreateBuilder())
            {
                var message = new Message("Hello world");
                var writer = builder.GetWriter();
                var inbox = builder.GetInbox();
                var task = inbox.ReceiveAsync(CancellationToken.None);
                Assert.IsFalse(task.IsCompleted);
                writer.Put(message);
                var received = task.GetAwaiter().GetResult();
                inbox.Delete(received);
                Assert.AreEqual(message, received);
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void Multithreaded()
        {
            using (var builder = CreateBuilder())
            {
                var message1 = new Message("Message 1");
                var message2 = new Message("Message 2");
                var writer1 = builder.GetWriter();
                var writer2 = builder.GetWriter();
                var inbox = builder.GetInbox();
                var task = inbox.ReceiveAsync(CancellationToken.None);
                Task.WaitAll(
                    Task.Factory.StartNew(() => writer1.Put(message1)),
                    Task.Factory.StartNew(() => writer2.Put(message2))
                    );
                var result1 = task.GetAwaiter().GetResult();
                var result2 = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
                CollectionAssert.AreEquivalent(new[] { message1, message2 }, new[] { result1, result2 });
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void Cancellable()
        {
            using (var builder = CreateBuilder())
            {
                var inbox = builder.GetInbox();
                var cancel = new CancellationTokenSource();
                var task = inbox.ReceiveAsync(cancel.Token);
                cancel.Cancel();
                try
                {
                    task.GetAwaiter().GetResult();
                    Assert.Fail("Expected OperationCanceledException");
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        [TestMethod]
        [Timeout(1000)]
        public void PutBackForRetry()
        {
            using (var builder = CreateBuilder())
            {
                var original = new Message("Hello");
                var writer = builder.GetWriter();
                var inbox = builder.GetInbox();
                writer.Put(original);
                var received = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
                received.Headers.RetryNumber = 1;
                inbox.Put(received);
                inbox.Delete(original);
                var retried = inbox.ReceiveAsync(CancellationToken.None).GetAwaiter().GetResult();
                Assert.AreEqual("Hello", retried.Payload);
                Assert.AreEqual(1, retried.Headers.RetryNumber);
            }
        }
    }

    [TestClass]
    public class MessageInboxMemoryTest : MessageInboxTestBase
    {
        protected override IMessageInboxTestBuilder CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IMessageInboxTestBuilder
        {
            private MemoryInbox _inbox = new MemoryInbox();

            public IMessageInboxWriter GetWriter()
            {
                return _inbox;
            }

            public IMessageInboxReader GetInbox()
            {
                return _inbox;
            }

            public void Dispose()
            {
            }
        }
    }
}
