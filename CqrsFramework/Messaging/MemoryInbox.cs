using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public class MemoryInbox : IMessageInboxReader
    {
        private object _lock = new object();
        private Queue<Message> _data;
        private TaskCompletionSource<Message> _task;
        private CancellationTokenRegistration _taskRegistration;

        public MemoryInbox()
        {
            _data = new Queue<Message>();
        }

        public void Delete(Message message)
        {
        }

        public Task<Message> ReceiveAsync(CancellationToken token)
        {
            lock (_lock)
            {
                if (_task != null)
                    throw new InvalidOperationException();
                if (_data.Count > 0)
                    return Task.FromResult(_data.Dequeue());
                _task = new TaskCompletionSource<Message>();
                _taskRegistration = token.Register(HandleCancel);
                return _task.Task;
            }
        }

        private void HandleCancel()
        {
            lock (_lock)
            {
                _task.TrySetCanceled();
                _task = null;
            }
        }

        public void Put(Message message)
        {
            TaskCompletionSource<Message> task = null;
            lock (_lock)
            {
                if (_task == null)
                    _data.Enqueue(message);
                else
                {
                    task = _task;
                    _taskRegistration.Dispose();
                    _task = null;
                }
            }
            if (task != null)
                task.TrySetResult(message);
        }
    }
}
