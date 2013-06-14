using CqrsFramework.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.ServiceBus
{
    public interface IPrioritizedInboxesReceiver : IEnumerable<PrioritizedInboxesPair>
    {
        Task<MessageWithSource> ReceiveAsync(CancellationToken token);
        void PutToDelayed(DateTime executeOn, MessageWithSource message);
    }

    public class MessageWithSource
    {
        private readonly int _key;
        private readonly IMessageInboxReader _inbox;
        private readonly Message _message;

        public MessageWithSource(int key, IMessageInboxReader inbox, Message message)
        {
            this._key = key;
            this._inbox = inbox;
            this._message = message;
        }

        public int Key { get { return _key; } }
        public IMessageInboxReader Inbox { get { return _inbox; } }
        public Message Message { get { return _message; } }
    }

    public class PrioritizedInboxesReceiver : IPrioritizedInboxesReceiver
    {
        private Dictionary<int, PrioritizedInboxesPair> _pairsByKey;
        private List<PrioritizedInboxesPair> _sortedPairs;
        private TaskCompletionSource<MessageWithSource> _taskSource;

        public PrioritizedInboxesReceiver()
        {
            _pairsByKey = new Dictionary<int, PrioritizedInboxesPair>();
            _sortedPairs = new List<PrioritizedInboxesPair>();
        }

        public PrioritizedInboxesPair this[int key]
        {
            get { return _pairsByKey[key]; }
        }

        public Task<MessageWithSource> ReceiveAsync(CancellationToken token)
        {
            _taskSource = new TaskCompletionSource<MessageWithSource>();
            token.Register(() => _taskSource.TrySetCanceled());
            foreach (var pair in _sortedPairs)
            {
                if (pair.InboxTask == null)
                {
                    pair.InboxTask = pair.Inbox.ReceiveAsync(token);
                    pair.InboxTask.ContinueWith(FinishedInbox, pair, TaskContinuationOptions.ExecuteSynchronously);
                }
                else if (pair.InboxTask.IsCompleted)
                    FinishedInbox(pair.InboxTask, pair);

                if (pair.DelayedTask == null)
                {
                    pair.DelayedTask = pair.Delayed.ReceiveAsync(token);
                    pair.DelayedTask.ContinueWith(FinishedDelayed, pair, TaskContinuationOptions.ExecuteSynchronously);
                }
                else if (pair.DelayedTask.IsCompleted)
                    FinishedDelayed(pair.DelayedTask, pair);
            }
            return _taskSource.Task;
        }

        private void FinishedInbox(Task<Message> task, object state)
        {
            var pair = state as PrioritizedInboxesPair;
            if (task.IsCanceled)
            {
                if (_taskSource.TrySetCanceled())
                    pair.InboxTask = null;
            }
            else
            {
                var result = new MessageWithSource(pair.Key, pair.Inbox, task.Result);
                if (_taskSource.TrySetResult(result))
                    pair.InboxTask = null;
            }
        }

        private void FinishedDelayed(Task<Message> task, object state)
        {
            var pair = state as PrioritizedInboxesPair;
            if (task.IsCanceled)
            {
                if (_taskSource.TrySetCanceled())
                    pair.DelayedTask = null;
            }
            else
            {
                var result = new MessageWithSource(pair.Key, pair.Inbox, task.Result);
                if (_taskSource.TrySetResult(result))
                    pair.DelayedTask = null;
            }
        }

        public void PutToDelayed(DateTime executeOn, MessageWithSource message)
        {
            var pair = _pairsByKey[message.Key];
            pair.Delayed.Add(executeOn, message.Message);
        }

        public void AddInboxPair(int key, int priority, IMessageInboxReader inbox, IDelayedMessages delayed)
        {
            var pair = new PrioritizedInboxesPair
            {
                Key = key,
                Priority = priority,
                Inbox = inbox,
                Delayed = delayed
            };
            _pairsByKey.Add(key, pair);
            _sortedPairs.Add(pair);
            _sortedPairs.Sort((a, b) => -a.Priority.CompareTo(b.Priority));
        }

        public IEnumerator<PrioritizedInboxesPair> GetEnumerator()
        {
            return _sortedPairs.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class PrioritizedInboxesPair
    {
        public IMessageInboxReader Inbox;
        public IDelayedMessages Delayed;
        public int Key;
        public int Priority;
        public Task<Message> InboxTask;
        public Task<Message> DelayedTask;
    }
}
