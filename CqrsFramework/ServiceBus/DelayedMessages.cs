using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.ServiceBus
{
    public interface IDelayedMessages
    {
        Task<Message> ReceiveAsync(CancellationToken cancellationToken);
        void Add(DateTime time, Message message);
    }

    public class DelayedMessages : IDelayedMessages
    {
        private object _lock = new object();
        private ITimeProvider _time;
        private TaskCompletionSource<Message> _taskSource;
        private Queue<Message> _ready;
        private List<ScheduledMessage> _messages;
        private Task _timerTask;
        private DateTime _timerTime;
        private CancellationTokenSource _cancel;
        private CancellationToken _token;

        private class ScheduledMessage
        {
            public DateTime Time;
            public Message Message;
            public ScheduledMessage(DateTime time, Message message)
            {
                this.Time = time;
                this.Message = message;
            }
        }

        public DelayedMessages(ITimeProvider time)
        {
            _time = time;
            _ready = new Queue<Message>();
            _messages = new List<ScheduledMessage>();
        }

        public Task<Message> ReceiveAsync(CancellationToken token)
        {
            lock (_lock)
            {
                if (_ready.Count > 0)
                    return Task.FromResult(_ready.Dequeue());
                _taskSource = new TaskCompletionSource<Message>();
                _token = token;
                _token.Register(CancelHandler);
                return _taskSource.Task;
            }
        }

        private void CancelHandler()
        {
            TaskCompletionSource<Message> sourceToCancel = null;
            CancellationTokenSource cancelToCancel = null;
            lock (_lock)
            {
                sourceToCancel = _taskSource;
                _taskSource = null;
                cancelToCancel = _cancel;
                _cancel = null;
            }
            if (sourceToCancel != null)
                sourceToCancel.TrySetCanceled();
            if (cancelToCancel != null)
                cancelToCancel.Cancel();
        }

        public void Add(DateTime time, Message message)
        {
            TaskCompletionSource<Message> taskSource = null;
            CancellationTokenSource cancelSource = null;
            lock (_lock)
            {
                if (time <= _time.Get())
                {
                    taskSource = _taskSource;
                    if (_taskSource == null)
                        _ready.Enqueue(message);
                    else
                        _taskSource = null;
                }
                else
                {
                    var scheduledMessage = new ScheduledMessage(time, message);
                    _messages.Add(scheduledMessage);
                    if (_timerTask == null)
                    {
                        _timerTime = time;
                        _cancel = new CancellationTokenSource();
                        _timerTask = _time.WaitUntil(time, _cancel.Token);
                        _timerTask.ContinueWith(TimerFinished, TaskContinuationOptions.ExecuteSynchronously);
                    }
                    else if (_timerTime > time)
                    {
                        cancelSource = _cancel;
                        _timerTime = time;
                        _cancel = new CancellationTokenSource();
                        _timerTask = _time.WaitUntil(time, _cancel.Token);
                        _timerTask.ContinueWith(TimerFinished, TaskContinuationOptions.ExecuteSynchronously);
                    }
                }
            }
            if (cancelSource != null)
                cancelSource.Cancel();
            if (taskSource != null)
                taskSource.SetResult(message);
        }

        private void TimerFinished(Task task)
        {
            if (task.IsCanceled)
                return;
            TaskCompletionSource<Message> taskSource = null;
            Message messageAsResult = null;
            lock (_lock)
            {
                var now = _time.Get();
                var toReady = _messages.Where(m => m.Time <= now).OrderBy(m => m.Time).Select(m => m.Message).ToList();
                foreach (var message in toReady)
                    _ready.Enqueue(message);
                _messages.RemoveAll(m => m.Time <= now);
                if (_taskSource != null && _ready.Count != 0)
                {
                    messageAsResult = _ready.Dequeue();
                    taskSource = _taskSource;
                    _taskSource = null;
                }
                var nextMessage = _messages.OrderBy(m => m.Time).FirstOrDefault();
                if (nextMessage == null)
                    _timerTask = null;
                else
                {
                    _timerTime = nextMessage.Time;
                    _cancel = new CancellationTokenSource();
                    _timerTask = _time.WaitUntil(nextMessage.Time, _cancel.Token);
                    _timerTask.ContinueWith(TimerFinished, TaskContinuationOptions.ExecuteSynchronously);
                }
            }
            if (taskSource != null)
                taskSource.SetResult(messageAsResult);
        }
    }
}
