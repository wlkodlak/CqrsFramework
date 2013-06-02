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
            _cancel = new CancellationTokenSource();
            _time.Setup(t => t.Get()).Returns(() => _now);
        }

        private void ProcessSingle()
        {
            var process = new DispatcherProcessCore(_cancel.Token, _receiver.Object, _dispatcher.Object, _errors.Object, _time.Object);
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
            _dispatcher.Setup(d => d.Dispatch(message)).Verifiable();
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
            _dispatcher.Setup(d => d.Dispatch(message)).Throws(exception).Verifiable();
            _errors.Setup(e => e.HandleException(_inbox.Object, message, exception)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void MessageWithTimeout()
        {
            var message = BuildMessage();
            message.Headers.TimeToLive = TimeSpan.FromSeconds(30);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _dispatcher.Setup(d => d.Dispatch(message)).Verifiable();
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void TimedOutMessage()
        {
            var message = BuildMessage();
            message.Headers.TimeToLive = TimeSpan.FromSeconds(10);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var exception = new ArgumentOutOfRangeException();
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _inbox.Setup(i => i.Delete(message)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void DelayMessage()
        {
            var message = BuildMessage();
            message.Headers.Delay = TimeSpan.FromSeconds(40);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var executeOn = message.Headers.CreatedOn.Add(message.Headers.Delay);
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _receiver.Setup(i => i.PutToDelayed(executeOn, msgsrc)).Verifiable();
            ProcessSingle();
        }

        [TestMethod]
        public void DelayedButCurrentMessage()
        {
            var message = BuildMessage();
            message.Headers.Delay = TimeSpan.FromSeconds(10);
            var msgsrc = new MessageWithSource(0, _inbox.Object, message);
            var executeOn = message.Headers.CreatedOn.Add(message.Headers.Delay);
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(msgsrc)).Verifiable();
            _receiver.Setup(i => i.PutToDelayed(executeOn, msgsrc));
            _dispatcher.Setup(d => d.Dispatch(message));
            _inbox.Setup(i => i.Delete(message));
            ProcessSingle();
        }

        [TestMethod]
        [Timeout(1000)]
        public void Cancelable()
        {
            var taskSource = new TaskCompletionSource<MessageWithSource>();
            _receiver.Setup(i => i.ReceiveAsync(_cancel.Token)).Returns(taskSource.Task).Verifiable();
            var process = new DispatcherProcessCore(_cancel.Token, _receiver.Object, _dispatcher.Object, _errors.Object, _time.Object);
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
