using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests
{
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
            var ready = _tasks.Where(t => t.Time <= _now && !t.Cancelled).OrderBy(t => t.Time).ToList();
            _tasks.RemoveAll(t => t.Time <= _now);
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
