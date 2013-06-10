using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;
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
        private enum State { Nowait, EmptyWait, TimerWait };

        private object _lock = new object();
        private State _state = State.Nowait;
        private ITimeProvider _time;
        private Queue<Message> _ready = new Queue<Message>();
        private OrderedMessageList _future = new OrderedMessageList();

        private TaskCompletionSource<Message> _receiveTask;
        private CancellationToken _receiveToken;
        private CancellationTokenRegistration _receiveCancelRegistration;
        
        private Task _timerTask;
        private CancellationTokenSource _timerTaskCancel;
        private DateTime _timerTaskTime;

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

        private class OrderedMessageList
        {
            private List<ScheduledMessage> _list = new List<ScheduledMessage>();

            public void Add(ScheduledMessage message)
            {
                int position = _list.Count;
                for (int i = 0; i < _list.Count; i++)
                {
                    if (_list[i].Time > message.Time)
                    {
                        position = i;
                        break;
                    }
                }
                _list.Insert(position, message);
            }
            public int Count { get { return _list.Count; } }
            public void RemoveOlder(DateTime time)
            {
                _list.RemoveAll(m => m.Time <= time);
            }
            public List<ScheduledMessage> GetOlder(DateTime time)
            {
                return _list.FindAll(m => m.Time <= time);
            }
            public ScheduledMessage FirstOrDefault()
            {
                return _list.Count == 0 ? null : _list[0];
            }
            private int Comparison(ScheduledMessage a, ScheduledMessage b)
            {
                return a.Time.CompareTo(b.Time);
            }
        }

        public DelayedMessages(ITimeProvider time)
        {
            _time = time;
        }

        public Task<Message> ReceiveAsync(CancellationToken token)
        {
            lock (_lock)
            {
                if (_state == State.Nowait)
                {
                    if (_ready.Count > 0)
                        return Task.FromResult(_ready.Dequeue());
                    else if (_future.Count == 0)
                    {
                        _receiveToken = token;
                        _receiveTask = new TaskCompletionSource<Message>();
                        _receiveCancelRegistration = _receiveToken.Register(HandleCancel);
                        _state = State.EmptyWait;
                        return _receiveTask.Task;
                    }
                    else
                    {
                        _state = State.TimerWait;
                        _receiveToken = token;
                        _receiveTask = new TaskCompletionSource<Message>();
                        _receiveCancelRegistration = _receiveToken.Register(HandleCancel);
                        _timerTaskCancel = new CancellationTokenSource();
                        _timerTaskTime = _future.FirstOrDefault().Time;
                        SetupTimerTask();
                        return _receiveTask.Task;
                    }
                }
                else
                    throw new InvalidOperationException();
            }
        }

        private void HandleCancel()
        {
            TaskCompletionSource<Message> cancelReceiveTask = null;
            lock (_lock)
            {
                if (_state == State.Nowait)
                    return;
                else if (_state == State.EmptyWait)
                {
                    cancelReceiveTask = _receiveTask;
                    _receiveTask = null;
                }
                else if (_state == State.TimerWait)
                {
                    cancelReceiveTask = _receiveTask;
                    _timerTaskCancel.Cancel();
                    _receiveTask = null;
                    _timerTask = null;
                    _timerTaskCancel = null;
                }
                _state = State.Nowait;
            }
            if (cancelReceiveTask != null)
                cancelReceiveTask.TrySetCanceled();
        }

        private void TimerFinished(Task task)
        {
            if (task.IsCanceled)
                return;
            TaskCompletionSource<Message> resultTask;
            Message resultMessage = null;
            Exception exception = null;

            lock (_lock)
            {
                try
                {
                    if (_state != State.TimerWait || task != _timerTask)
                        return;
                    var now = _time.Get();
                    foreach (var message in _future.GetOlder(now))
                        _ready.Enqueue(message.Message);
                    _future.RemoveOlder(now);
                    _timerTaskCancel.Dispose();
                    _receiveCancelRegistration.Dispose();
                    resultTask = _receiveTask;
                    resultMessage = _ready.Dequeue();
                    _state = State.Nowait;
                }
                catch (Exception ex)
                {
                    _timerTaskCancel.Dispose();
                    _receiveCancelRegistration.Dispose();
                    resultTask = _receiveTask;
                    exception = ex;
                    _state = State.Nowait;
                }
            }

            if (resultMessage != null)
                resultTask.TrySetResult(resultMessage);
            else if (exception != null)
                resultTask.SetException(exception);
        }

        public void Add(DateTime time, Message message)
        {
            TaskCompletionSource<Message> taskForResult = null;
            lock (_lock)
            {
                var now = _time.Get();
                bool isCurrent = time <= now;
                if (_state == State.Nowait)
                {
                    if (isCurrent)
                        _ready.Enqueue(message);
                    else
                        _future.Add(new ScheduledMessage(time, message));
                }
                else if (_state == State.EmptyWait)
                {
                    if (isCurrent)
                    {
                        taskForResult = _receiveTask;
                        _receiveCancelRegistration.Dispose();
                        _state = State.Nowait;
                    }
                    else
                    {
                        _future.Add(new ScheduledMessage(time, message));
                        _state = State.TimerWait;
                        _timerTaskTime = time;
                        _timerTaskCancel = new CancellationTokenSource();
                        SetupTimerTask();
                    }
                }
                else
                {
                    if (isCurrent)
                    {
                        _state = State.Nowait;
                        _timerTaskCancel.Cancel();
                        _receiveCancelRegistration.Dispose();
                        taskForResult = _receiveTask;
                    }
                    else if (time < _timerTaskTime)
                    {
                        _future.Add(new ScheduledMessage(time, message));
                        _timerTaskCancel.Cancel();
                        _timerTaskCancel = new CancellationTokenSource();
                        _timerTaskTime = time;
                        SetupTimerTask();
                    }
                    else
                        _future.Add(new ScheduledMessage(time, message));
                }
            }
            if (taskForResult != null)
                taskForResult.TrySetResult(message);
        }

        private void SetupTimerTask()
        {
            var scheduler = TaskScheduler.Current;
            _timerTask = _time.WaitUntil(_timerTaskTime, _timerTaskCancel.Token);
            _timerTask.ContinueWith(TimerFinished, _timerTaskCancel.Token, TaskContinuationOptions.ExecuteSynchronously, scheduler);
        }
    }
}
