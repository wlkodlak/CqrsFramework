﻿using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using CqrsFramework;
using CqrsFramework.ServiceBus;
using System.Linq;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
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
            ScheduleMessage(delayed, messages[0], true, messages[1]);
            ScheduleMessage(delayed, messages[2], false, null);
            MoveTime(messages[0]);
            Assert.AreSame(messages[0], task1.GetAwaiter().GetResult());
            MoveTime(messages[1]);
            Assert.AreSame(messages[1], delayed.ReceiveAsync(_cancel.Token).GetAwaiter().GetResult());
            MoveTime(messages[2]);
            Assert.AreSame(messages[2], delayed.ReceiveAsync(_cancel.Token).GetAwaiter().GetResult());
            ScheduleMessage(delayed, messages[3], true, null);
            MoveTime(messages[3]);
            Assert.AreSame(messages[3], delayed.ReceiveAsync(_cancel.Token).GetAwaiter().GetResult());
        }

        private void MoveTime(Message message)
        {
            var time = message.Headers.CreatedOn.Add(message.Headers.Delay);
            _time.ChangeTime(time);
        }

        private void ScheduleMessage(DelayedMessages delayed, Message message, bool shouldSetTimer, Message cancelledTime)
        {
            var time = message.Headers.CreatedOn.Add(message.Headers.Delay);
            delayed.Add(time, message);
            _time.Verify(time, shouldSetTimer ? 1 : 0);
            if (cancelledTime != null)
            {
                var previousTime = cancelledTime.Headers.CreatedOn.Add(cancelledTime.Headers.Delay);
                _time.VerifyCancelled(previousTime, 1);
            }
        }

        private Message BuildMessage(object contents, DateTime created, DateTime scheduled)
        {
            var message = new Message(contents);
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.CreatedOn = created;
            message.Headers.Delay = scheduled - created;
            return message;
        }
    }

    public class TestTimeProvider : ITimeProvider
    {
        private DateTime _now;
        private List<WaitingTask> _tasks;
        private List<WaitingTask> _verifiable;

        private class WaitingTask
        {
            public DateTime Time;
            public TaskCompletionSource<object> TaskSource;
            public bool Cancelled;
        }

        public TestTimeProvider(DateTime initialTime)
        {
            _now = initialTime;
            _tasks = new List<WaitingTask>();
            _verifiable = new List<WaitingTask>();
        }

        public void ChangeTime(DateTime newTime)
        {
            _now = newTime;
            var ready = _tasks.Where(t => t.Time >= _now).OrderBy(t => t.Time).ToList();
            _tasks.RemoveAll(t => t.Time >= _now);
            foreach (var task in ready)
                task.TaskSource.TrySetResult(null);
        }

        public DateTime Get()
        {
            return _now;
        }

        public DateTime Now { get { return _now; } }

        public Task WaitUntil(DateTime time, CancellationToken cancel)
        {
            var task = new WaitingTask();
            task.Time = time;
            if (time <= _now)
            {
                _verifiable.Add(task);
                return Task.FromResult((object)null);
            }
            else
            {
                task.TaskSource = new TaskCompletionSource<object>();
                cancel.Register(new CancelledByToken(task).Handler);
                _tasks.Add(task);
                _verifiable.Add(task);
                return task.TaskSource.Task;
            }
        }

        private class CancelledByToken
        {
            private WaitingTask _task;
            public CancelledByToken(WaitingTask task) { this._task = task; }
            public void Handler() { _task.Cancelled = true; _task.TaskSource.TrySetCanceled(); }
        }

        public void Verify(DateTime dateTime, int count)
        {
            var realCount = _verifiable.Count(t => t.Time == dateTime && !t.Cancelled);
            Assert.AreEqual(count, realCount, "ITimeProvider.WaitUntil({0}) calls count", dateTime);
        }

        public void VerifyCancelled(DateTime dateTime, int count)
        {
            var realCount = _verifiable.Count(t => t.Time == dateTime && t.Cancelled);
            Assert.AreEqual(count, realCount, "Cancelled tasks count for {0}", dateTime);
        }
    }
}
