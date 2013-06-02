using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CqrsFramework;
using CqrsFramework.ServiceBus;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class DispatcherProcessTest
    {
        private CancellationTokenSource _cancel;
        private MockRepository _repo;
        private Mock<ITimeProvider> _time;
        private Mock<IMessageInboxReader> _inbox;
        private Mock<IPrioritizedInboxesReceiver> _receiver;
        private Mock<IMessageErrorPolicy> _errors;
        private Mock<IMessageDispatcher> _dispatcher;
        private Mock<IMessageDeduplicator> _dup;
        private DateTime _now = new DateTime(2013, 6, 1, 18, 22, 33);

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _time = _repo.Create<ITimeProvider>();
            _inbox = _repo.Create<IMessageInboxReader>();
            _receiver = _repo.Create<IPrioritizedInboxesReceiver>();
            _errors = _repo.Create<IMessageErrorPolicy>();
            _dispatcher = _repo.Create<IMessageDispatcher>();
            _dup = _repo.Create<IMessageDeduplicator>();
            _cancel = new CancellationTokenSource();
            _time.Setup(t => t.Get()).Returns(() => _now);
        }

        private void ProcessSingle()
        {
            var process = new DispatcherProcessCore(_cancel.Token, _receiver.Object, _dispatcher.Object, _errors.Object, _time.Object, _dup.Object);
            var processTask = process.ProcessSingle();
            processTask.GetAwaiter().GetResult();
            _repo.Verify();
        }

        [TestMethod]
        public void ReceivesSingleMessage()
        {
            var message = BuildMessage();
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false).Verifiable();
            _dispatcher.Setup(d => d.Dispatch(message)).Verifiable();
            _dup.Setup(d => d.MarkHandled(message)).Verifiable();
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void HandlesDispatchErrors()
        {
            var message = BuildMessage();
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var exception = new ArgumentOutOfRangeException();
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false).Verifiable();
            _dispatcher.Setup(d => d.Dispatch(message)).Throws(exception).Verifiable();
            _errors.Setup(e => e.HandleException(_inbox.Object, message, exception)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void MessageWithTimeout()
        {
            var message = BuildMessage();
            message.Headers.ValidUntil = _now.AddSeconds(30);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false).Verifiable();
            _dispatcher.Setup(d => d.Dispatch(message)).Verifiable();
            _dup.Setup(d => d.MarkHandled(message)).Verifiable();
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void TimedOutMessage()
        {
            var message = BuildMessage();
            message.Headers.ValidUntil = _now.AddSeconds(-5);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var exception = new ArgumentOutOfRangeException();
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false);
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void DelayMessage()
        {
            var message = BuildMessage();
            message.Headers.DeliverOn = _now.AddSeconds(40);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var executeOn = message.Headers.DeliverOn;
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false);
            _receiver.Setup(i => i.PutToDelayed(executeOn, msgsrc)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void DelayedButCurrentMessage()
        {
            var message = BuildMessage();
            message.Headers.DeliverOn = _now.AddSeconds(10);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var executeOn = message.Headers.DeliverOn;
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _receiver.Setup(i => i.PutToDelayed(executeOn, msgsrc));
            _dup.Setup(d => d.IsDuplicate(message)).Returns(false);
            _dispatcher.Setup(d => d.Dispatch(message));
            _dup.Setup(d => d.MarkHandled(message));
            _inbox.Setup(i => i.Delete(message));
            ProcessSingle();
        }

        [TestMethod]
        public void DuplicatedMessage()
        {
            var message = BuildMessage();
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var executeOn = message.Headers.DeliverOn;
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dup.Setup(d => d.IsDuplicate(message)).Returns(true).Verifiable();
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        [Timeout(1000)]
        public void Cancelable()
        {
            var taskSource = new TaskCompletionSource<MessageWithSource>();
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(taskSource.Task).Verifiable();
            var process = new DispatcherProcessCore(_cancel.Token, _receiver.Object, _dispatcher.Object, _errors.Object, _time.Object, _dup.Object);
            var processTask = process.ProcessSingle();
            Assert.IsFalse(processTask.IsCompleted, "Task should wait for message");
            _cancel.Token.Register(() => taskSource.TrySetCanceled());
            _cancel.Cancel();
            try
            {
                processTask.GetAwaiter().GetResult();
                Assert.Fail("It should have been cancelled");
            }
            catch (OperationCanceledException)
            {
            }
        }

        private Message BuildMessage()
        {
            var message = new Message("Hello world");
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-15);
            return message;
        }
    }
}
