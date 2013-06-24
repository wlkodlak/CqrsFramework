using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Infrastructure;
using CqrsFramework.EventStore;
using CqrsFramework.Messaging;
using System.Threading;
using CqrsFramework.Serialization;

namespace CqrsFramework.ServiceBus
{
    public class ProjectionProcess
    {
        private IEventStoreReader _store;
        private CancellationToken _token;
        private ITimeProvider _time;
        private IMessageErrorPolicy _error;
        private IMessageSerializer _serializer;
        private int _pollingCount;
        private TimeSpan _pollingInterval;

        private TaskCompletionSource<object> _task;
        private long _clock;
        private ProcessStatus _status;
        private List<ProjectionInfo> _projections;
        private IEnumerator<EventStoreEvent> _inputEnumerator;

        private enum ProcessStatus { Registering, Updating, Finished, Waiting }
        private enum ProjectionStatus { Registered, RebuildStarted, Updating, UpdateFinished, Failed }

        private class ProjectionInfo
        {
            public ProjectionStatus Status;
            public IProjectionDispatcher Handler;
            public bool NeedsRebuild;
            public long StartingClock;
        }

        private class DropAllErrorPolicy : IMessageErrorPolicy
        {
            public MessageErrorAction HandleException(int retryNumber, Exception exception)
            {
                return MessageErrorAction.Drop();
            }
        }

        public ProjectionProcess(IEventStoreReader store, CancellationToken token, ITimeProvider time, IMessageSerializer serializer)
        {
            _store = store;
            _token = token;
            _time = time;
            _serializer = serializer;
            _error = new DropAllErrorPolicy();
            _pollingCount = 1000;
            _pollingInterval = TimeSpan.FromMilliseconds(200);
            _token.Register(HandleCancel);
            _status = ProcessStatus.Registering;
            _projections = new List<ProjectionInfo>();
        }

        public ProjectionProcess WithInterval(TimeSpan interval)
        {
            _pollingInterval = interval;
            return this;
        }

        public ProjectionProcess WithBlockSize(int eventsCount)
        {
            _pollingCount = eventsCount;
            return this;
        }

        public ProjectionProcess WithErrorPolicy(IMessageErrorPolicy policy)
        {
            _error = policy;
            return this;
        }

        public void Register(IProjectionDispatcher projection)
        {
            var info = new ProjectionInfo();
            info.Status = ProjectionStatus.Registered;
            info.Handler = projection;
            info.NeedsRebuild = projection.NeedsRebuild();
            info.StartingClock = info.NeedsRebuild ? 0 : projection.GetClockToHandle();
            _projections.Add(info);
        }

        public Task ProcessNext()
        {
            if (_projections.Count == 0)
                throw new InvalidOperationException("Empty projection list");
            if (_status == ProcessStatus.Registering)
            {
                InitializeUpdate();
                _status = ProcessStatus.Updating;
            }
            else if (_status == ProcessStatus.Waiting)
                throw new InvalidOperationException("Cannot request waiting while in waiting state");

            var stored = GetNextEvent();
            if (stored == null)
            {
                if (_status == ProcessStatus.Updating)
                    FinishUpdate();
                _status = ProcessStatus.Waiting;
                return CreateWaitingTask();
            }
            else
            {
                return ProcessEvent(stored, _status == ProcessStatus.Updating);
            }
        }

        private Task CreateWaitingTask()
        {
            _task = new TaskCompletionSource<object>();
            var _timerTask = _time.WaitUntil(_time.Get().Add(_pollingInterval), _token);
            _timerTask.ContinueWith(HandleTimer, _token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Current);
            return _task.Task;
        }

        private Task ProcessEvent(EventStoreEvent stored, bool allowBatch)
        {
            var message = _serializer.Deserialize(stored.Data);
            message.Headers.EventClock = stored.Clock;
            foreach (var projection in _projections)
            {
                if (stored.Clock < projection.StartingClock)
                    continue;
                if (projection.Status == ProjectionStatus.Registered && allowBatch)
                {
                    projection.Handler.BeginUpdate();
                    projection.Status = ProjectionStatus.Updating;
                }
                else if (projection.Status == ProjectionStatus.Registered && !allowBatch)
                {
                    projection.Status = ProjectionStatus.UpdateFinished;
                }
                else if (projection.Status == ProjectionStatus.RebuildStarted)
                    projection.Status = ProjectionStatus.Updating;

                if (projection.Status == ProjectionStatus.Updating || projection.Status == ProjectionStatus.UpdateFinished)
                {
                    projection.Handler.Dispatch(message);
                    // TODO: error policy
                }
            }
            return Task.FromResult<object>(null);
        }

        private EventStoreEvent GetNextEvent()
        {
            if (_inputEnumerator != null && _inputEnumerator.MoveNext())
            {
                _clock++;
                return _inputEnumerator.Current;
            }
            _inputEnumerator = _store.GetSince(_clock, _pollingCount).GetEnumerator();
            if (!_inputEnumerator.MoveNext())
                return null;
            _clock++;
            return _inputEnumerator.Current;
        }

        private void InitializeUpdate()
        {
            _clock = long.MaxValue;
            foreach (var projection in _projections)
            {
                if (_clock > projection.StartingClock)
                    _clock = projection.StartingClock;
                if (projection.NeedsRebuild)
                {
                    projection.Status = ProjectionStatus.RebuildStarted;
                    projection.Handler.BeginUpdate();
                    projection.Handler.Reset();
                }
            }
        }

        private void FinishUpdate()
        {
            foreach (var projection in _projections)
            {
                if (projection.Status == ProjectionStatus.Updating /* || projection.Status == ProjectionStatus.RebuildStarted*/)
                {
                    projection.Handler.EndUpdate();
                    projection.Status = ProjectionStatus.UpdateFinished;
                }
                else if (projection.Status == ProjectionStatus.Registered || projection.Status == ProjectionStatus.RebuildStarted)
                    projection.Status = ProjectionStatus.UpdateFinished;
            }
        }

        private void HandleCancel()
        {
            var taskToCancel = _task;
            if (taskToCancel != null)
                taskToCancel.TrySetCanceled();
        }

        private void HandleTimer(Task timerTask)
        {
            if (timerTask.IsCanceled)
                return;
            var stored = GetNextEvent();
            if (stored != null)
            {
                ProcessEvent(stored, false);
                _status = ProcessStatus.Finished;
                _task.TrySetResult(null);
            }
            else
            {
                var _timerTask = _time.WaitUntil(_time.Get().Add(_pollingInterval), _token);
                _timerTask.ContinueWith(HandleTimer, _token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Current);
            }
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
