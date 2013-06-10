using System;
using CqrsFramework;
using CqrsFramework.ServiceBus;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.ServiceBus
{
    [TestClass]
    public class PriorityReceiverTest
    {
        private MockRepository _repo;
        private int[] _keys;
        private int[] _priorities;
        private Mock<IMessageInboxReader>[] _inbox;
        private Mock<IDelayedMessages>[] _delayed;
        private List<TaskCompletionSource<Message>> _tasks;
        private CancellationTokenSource _cancel;
        private DateTime _now = new DateTime(2013, 6, 1, 18, 22, 33);

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _keys = new int[] { 5, 9, 2, 7 };
            _priorities = new int[] { 11, 16, 28, 30 };
            _inbox = new Mock<IMessageInboxReader>[4];
            _delayed = new Mock<IDelayedMessages>[4];
            for (int i = 0; i < 4; i++)
            {
                _inbox[i] = _repo.Create<IMessageInboxReader>();
                _delayed[i] = _repo.Create<IDelayedMessages>();
            }
            _cancel = new CancellationTokenSource();
            _tasks = new List<TaskCompletionSource<Message>>();
        }

        [TestMethod]
        public void CanAddInbox()
        {
            var receiver = new PrioritizedInboxesReceiver();
            receiver.AddInboxPair(2, 27, _inbox[0].Object, _delayed[0].Object);
            PrioritizedInboxesPair queuePair = receiver[2];
            Assert.AreSame(_inbox[0].Object, queuePair.Inbox, "Inbox");
            Assert.AreSame(_delayed[0].Object, queuePair.Delayed, "Delayed");
            Assert.AreEqual(2, queuePair.Key, "Key");
            Assert.AreEqual(27, queuePair.Priority, "Priority");
        }

        [TestMethod]
        public void PairsAreSortedByPriority()
        {
            var receiver = new PrioritizedInboxesReceiver();
            receiver.AddInboxPair(2, 28, _inbox[2].Object, _delayed[2].Object);
            receiver.AddInboxPair(9, 16, _inbox[1].Object, _delayed[1].Object);
            receiver.AddInboxPair(7, 30, _inbox[3].Object, _delayed[3].Object);
            receiver.AddInboxPair(5, 11, _inbox[0].Object, _delayed[0].Object);
            var realKeys = receiver.Select(p => p.Key).ToArray();
            var expected = new int[] { 7, 2, 9, 5 };
            Assert.AreEqual(expected.Length, realKeys.Length, "Collection length");
            for (int i = 0; i < expected.Length; i++)
                Assert.AreEqual(expected[i], realKeys[i], "Collection keys {0}", i);
            AssertSameInboxPair(2, 28, _inbox[2].Object, _delayed[2].Object, receiver[2]);
            AssertSameInboxPair(9, 16, _inbox[1].Object, _delayed[1].Object, receiver[9]);
            AssertSameInboxPair(7, 30, _inbox[3].Object, _delayed[3].Object, receiver[7]);
            AssertSameInboxPair(5, 11, _inbox[0].Object, _delayed[0].Object, receiver[5]);
        }

        private void AssertSameInboxPair(int key, int prio, IMessageInboxReader inbox, IDelayedMessages delayed, PrioritizedInboxesPair pair)
        {
            Assert.AreEqual(key, pair.Key, "Key {0}", key);
            Assert.AreEqual(prio, pair.Priority, "Priority {0}", key);
            Assert.AreSame(inbox, pair.Inbox, "Inbox {0}", key);
            Assert.AreSame(delayed, pair.Delayed, "Delayed {0}", key);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReturnsMostPrioritizedAvailableMessage()
        {
            var message1 = BuildMessage("Hello");
            var message2 = BuildMessage("World");
            _inbox[3].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(message1)).Verifiable();
            _inbox[2].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(TaskForCancel()).Verifiable();
            _inbox[1].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(Task.FromResult(message2)).Verifiable();
            _inbox[0].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(TaskForCancel()).Verifiable();
            for (int i = 0; i < 4; i++)
                _delayed[i].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(TaskForCancel()).Verifiable();

            var receiver = CreateReceiver();
            var task = receiver.ReceiveAsync(_cancel.Token);
            var result = task.GetAwaiter().GetResult();
            _repo.Verify();
            Assert.AreEqual(7, result.Key, "Key");
            Assert.AreSame(_inbox[3].Object, result.Inbox, "Inbox");
            Assert.AreSame(message1, result.Message, "Message");
        }

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveMultipleMessages()
        {
            var message1 = BuildMessage("Msg1");
            var message2 = BuildMessage("Msg2");
            var message3 = BuildMessage("Msg3");
            var message4 = BuildMessage("Msg4");

            for (int i = 0; i < 12; i++)
                TaskForCancel();

            _delayed[0].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(_tasks[0].Task);
            _delayed[1].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(_tasks[1].Task);
            _delayed[2].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(_tasks[2].Task);
            _delayed[3].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(_tasks[3].Task);
            _inbox[3].Setup(r => r.ReceiveAsync(_cancel.Token)).Returns(_tasks[4].Task);
            _inbox[2].SetupSequence(r => r.ReceiveAsync(_cancel.Token))
                .Returns(_tasks[5].Task).Returns(_tasks[6].Task).Returns(_tasks[7].Task);
            _inbox[1].SetupSequence(r => r.ReceiveAsync(_cancel.Token))
                .Returns(_tasks[8].Task).Returns(_tasks[9].Task);
            _inbox[0].SetupSequence(r => r.ReceiveAsync(_cancel.Token))
                .Returns(_tasks[10].Task).Returns(_tasks[11].Task);

            var receiver = CreateReceiver();
            ExpectResult(1, receiver, 2, message1, 5);
            ExpectResult(2, receiver, 0, message2, 10);
            ExpectResult(3, receiver, 2, message3, 6);
            ExpectResult(4, receiver, 1, message4, 8);
        }

        private void ExpectResult(int round, PrioritizedInboxesReceiver receiver, int index, Message message, int taskIndex)
        {
            var task = receiver.ReceiveAsync(_cancel.Token);
            _tasks[taskIndex].SetResult(message);
            var result = task.GetAwaiter().GetResult();
            Assert.AreEqual(_keys[index], result.Key, "Key {0}", round);
            Assert.AreSame(_inbox[index].Object, result.Inbox, "Inbox {0}", round);
            Assert.AreSame(message, result.Message, "Message {0}", round);
        }

        [TestMethod]
        [Timeout(1000)]
        public void DelayedMessage()
        {
            var message1 = BuildMessage("Msg1");
            var message1on = message1.Headers.CreatedOn.AddSeconds(30);
            message1.Headers.DeliverOn = message1on;
            var message2 = BuildMessage("Msg2");

            for (int i = 0; i < 12; i++)
                TaskForCancel();

            SetupInboxTasks(3, 0);
            SetupInboxTasks(2, 1, 2);
            SetupInboxTasks(1, 3, 10);
            SetupInboxTasks(0, 4);
            SetupDelayedTasks(3, 5);
            SetupDelayedTasks(2, 6, 7);
            SetupDelayedTasks(1, 8);
            SetupDelayedTasks(0, 9);
            _delayed[2].Setup(d => d.Add(message1on, message1)).Verifiable();

            var receiver = CreateReceiver();

            _tasks[1].SetResult(message1);
            _tasks[3].SetResult(message2);

            var result1 = receiver.ReceiveAsync(_cancel.Token).GetAwaiter().GetResult();
            Assert.AreEqual(_keys[2], result1.Key);
            Assert.AreSame(message1, result1.Message);
            receiver.PutToDelayed(message1on, result1);
            _delayed[2].Verify(d => d.Add(message1on, message1), Times.Once());

            var result2 = receiver.ReceiveAsync(_cancel.Token).GetAwaiter().GetResult();
            Assert.AreEqual(_keys[1], result2.Key);
            Assert.AreSame(message2, result2.Message);

            var result3task = receiver.ReceiveAsync(_cancel.Token);
            _now = _now.AddSeconds(15);
            _tasks[6].SetResult(message1);
            var result3 = result3task.GetAwaiter().GetResult();
            Assert.AreEqual(_keys[2], result3.Key);
            Assert.AreSame(message1, result3.Message);
        }

        private void SetupInboxTasks(int inbox, params int[] tasks)
        {
            var mockSetup = _inbox[inbox].SetupSequence(i => i.ReceiveAsync(_cancel.Token));
            foreach (var task in tasks)
                mockSetup = mockSetup.Returns(_tasks[task].Task);
        }

        private void SetupDelayedTasks(int delayed, params int[] tasks)
        {
            var mockSetup = _delayed[delayed].SetupSequence(i => i.ReceiveAsync(_cancel.Token));
            foreach (var task in tasks)
                mockSetup = mockSetup.Returns(_tasks[task].Task);
        }

        [TestMethod]
        [Timeout(1000)]
        public void Cancellable()
        {
            for (int i = 0; i < 12; i++)
                TaskForCancel();

            SetupInboxTasks(3, 0);
            SetupInboxTasks(2, 1);
            SetupInboxTasks(1, 2);
            SetupInboxTasks(0, 3);
            SetupDelayedTasks(3, 4);
            SetupDelayedTasks(2, 5);
            SetupDelayedTasks(1, 6);
            SetupDelayedTasks(0, 7);
            
            var receiver = CreateReceiver();
            var task = receiver.ReceiveAsync(_cancel.Token);
            _cancel.Cancel();
            try
            {
                task.GetAwaiter().GetResult();
                Assert.Fail("OperationCanceledException expected");
            }
            catch (OperationCanceledException)
            {
            }
        }

        private Task<Message> TaskForCancel()
        {
            var taskSource = new TaskCompletionSource<Message>();
            _tasks.Add(taskSource);
            _cancel.Token.Register(() => taskSource.TrySetCanceled());
            return taskSource.Task;
        }

        private PrioritizedInboxesReceiver CreateReceiver()
        {
            var receiver = new PrioritizedInboxesReceiver();
            for (int i = _keys.Length - 1; i >= 0; i--)
                receiver.AddInboxPair(_keys[i], _priorities[i], _inbox[i].Object, _delayed[i].Object);
            return receiver;
        }

        private Message BuildMessage(object contents)
        {
            var message = new Message(contents);
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = _now.AddSeconds(-15);
            return message;
        }
    }
}
