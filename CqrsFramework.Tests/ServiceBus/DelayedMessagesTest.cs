using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using CqrsFramework;
using CqrsFramework.ServiceBus;
using System.Linq;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Messaging;

namespace CqrsFramework.Tests.ServiceBus
{
    [TestClass]
    public class DelayedMessagesTest
    {
        private CancellationTokenSource _cancel;
        private TestTimeProvider _time;

        [TestInitialize]
        public void Initialize()
        {
            _cancel = new CancellationTokenSource();
            _time = new TestTimeProvider(new DateTime(2013, 6, 1, 10, 33, 21));
        }

        [TestMethod]
        [Timeout(1000)]
        public void EmptyCancellable()
        {
            var delayed = new DelayedMessages(_time);
            var task = delayed.ReceiveAsync(_cancel.Token);
            Assert.IsFalse(task.IsCompleted, "Should not be completed before cancel");
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

        [TestMethod]
        [Timeout(1000)]
        public void ReceiveDelayedButCurrentMessage()
        {
            var message = BuildMessage("Hello World", _time.Now.AddSeconds(-5), _time.Now);
            var delayed = new DelayedMessages(_time);
            delayed.Add(_time.Now, message);
            var task = delayed.ReceiveAsync(_cancel.Token);
            var result = task.GetAwaiter().GetResult();
            Assert.AreSame(message, result);
        }

        [TestMethod]
        [Timeout(1000)]
        public void ScheduleToFuture()
        {
            var scheduledTime = _time.Now.AddSeconds(5);
            var message = BuildMessage("Hello World", _time.Now.AddSeconds(-5), scheduledTime);
            var delayed = new DelayedMessages(_time);
            delayed.Add(scheduledTime, message);
            var task = delayed.ReceiveAsync(_cancel.Token);
            _time.Verify(scheduledTime, 1);
            _time.ChangeTime(scheduledTime);
            var result = task.GetAwaiter().GetResult();
            Assert.AreSame(message, result);
        }

        [TestMethod]
        [Timeout(1000)]
        public void SingleTimerTask()
        {
            var messages = new Message[]
            {
                BuildMessage("Message 1", _time.Now, _time.Now.AddSeconds(10)),
                BuildMessage("Message 2", _time.Now, _time.Now.AddSeconds(15)),
                BuildMessage("Message 3", _time.Now, _time.Now.AddSeconds(18)),
                BuildMessage("Message 4", _time.Now, _time.Now.AddSeconds(20))
            };
            var delayed = new DelayedMessages(_time);
            ScheduleMessage(delayed, messages[1], true, null);
            var task1 = delayed.ReceiveAsync(_cancel.Token);
            VerifySchedule(messages[1], true);
            ScheduleMessage(delayed, messages[0], true, messages[1]);
            ScheduleMessage(delayed, messages[2], false, null);
            VerifySchedule(messages[2], false);
            MoveTime(messages[0]);
            Assert.AreSame(messages[0], task1.GetAwaiter().GetResult());
            MoveTime(messages[1]);
            var task2 = delayed.ReceiveAsync(_cancel.Token);
            VerifySchedule(messages[1], true);
            Assert.AreSame(messages[1], task2.GetAwaiter().GetResult());
            MoveTime(messages[2]);
            var task3 = delayed.ReceiveAsync(_cancel.Token);
            VerifySchedule(messages[2], true);
            Assert.AreSame(messages[2], task3.GetAwaiter().GetResult());
            ScheduleMessage(delayed, messages[3], true, null);
            var task4 = delayed.ReceiveAsync(_cancel.Token);
            VerifySchedule(messages[3], true);
            MoveTime(messages[3], true);
            MoveTime(messages[3], false);
            Assert.AreSame(messages[3], task4.GetAwaiter().GetResult());
        }

        private void MoveTime(Message message, bool justBefore = false)
        {
            var newTime = message.Headers.DeliverOn;
            if (justBefore)
                newTime = newTime.AddMilliseconds(-500);
            _time.ChangeTime(newTime);
        }

        private void ScheduleMessage(DelayedMessages delayed, Message message, bool shouldSetTimer, Message cancelledTime)
        {
            delayed.Add(message.Headers.DeliverOn, message);
            if (cancelledTime != null)
            {
                _time.Verify(message.Headers.DeliverOn, shouldSetTimer ? 1 : 0);
                _time.VerifyCancelled(cancelledTime.Headers.DeliverOn, 1);
            }
        }

        private void VerifySchedule(Message message, bool shouldSetTimer)
        {
            _time.Verify(message.Headers.DeliverOn, shouldSetTimer ? 1 : 0);
        }

        private Message BuildMessage(object contents, DateTime created, DateTime scheduled)
        {
            var message = new Message(contents);
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = created;
            message.Headers.DeliverOn = scheduled;
            return message;
        }
    }
}
