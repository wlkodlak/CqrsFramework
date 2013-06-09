using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.InTable
{
    public class TableMessageInbox : IMessageInboxReader
    {
        private ITableProvider _table;
        private string _queueName;
        private IMessageSerializer _serializer;
        private ITimeProvider _time;

        private object _lock = new object();
        private TaskCompletionSource<Message> _task;
        private CancellationTokenRegistration _cancelRegistration;
        private readonly TimeSpan _checkInterval = TimeSpan.FromMilliseconds(100);
        private Task _timerTask;
        private Dictionary<Guid, TableProviderRow> _receivedMessages = new Dictionary<Guid, TableProviderRow>();
        private Queue<TableProviderRow> _messagesForReceive = new Queue<TableProviderRow>();
        private bool _oldMessagesLoaded = false;

        public TableMessageInbox(ITableProvider table, string queueName, IMessageSerializer serializer, ITimeProvider time)
        {
            _table = table;
            _queueName = queueName;
            _serializer = serializer;
            _time = time;
        }

        public void Delete(Message message)
        {
            var guid = message.Headers.MessageId;
            TableProviderRow oldRow;
            if (_receivedMessages.TryGetValue(guid, out oldRow))
                _table.Delete(oldRow);
        }

        public Task<Message> ReceiveAsync(CancellationToken token)
        {
            lock (_lock)
            {
                _task = new TaskCompletionSource<Message>();
                var message = GetNextMessage();
                if (message == null)
                {
                    _cancelRegistration = token.Register(CancelHandler);
                    _timerTask = _time.WaitUntil(_time.Get().Add(_checkInterval), token);
                    _timerTask.ContinueWith(TimerHandler, token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Current);
                }
                else
                {
                    _task.SetResult(message);
                }
                return _task.Task;
            }
        }

        private Message GetNextMessage()
        {
            TableProviderRow receivedRow = null;
            if (_messagesForReceive.Count > 0)
                receivedRow = _messagesForReceive.Dequeue();
            else
            {
                var filterable = _table.GetRows();
                if (_queueName != null)
                    filterable = filterable.Where("queue").Is(_queueName);
                if (_oldMessagesLoaded)
                    filterable = filterable.Where("status").Is(0);
                foreach (var row in filterable)
                    _messagesForReceive.Enqueue(row);
                _oldMessagesLoaded = true;
                if (_messagesForReceive.Count > 0)
                    receivedRow = _messagesForReceive.Dequeue();
            }

            if (receivedRow != null)
            {
                receivedRow["status"] = 1;
                _table.Update(receivedRow);
                var message = MessageFromRow(receivedRow);
                _receivedMessages[message.Headers.MessageId] = receivedRow;
                return message;
            }
            else
                return null;
        }

        private Message MessageFromRow(TableProviderRow row)
        {
            return (row == null) ? null : _serializer.Deserialize(row.Get<byte[]>("data"));
        }

        private void CancelHandler()
        {
            _task.SetCanceled();
        }

        private void TimerHandler(Task timerTask)
        {
            if (timerTask != _timerTask || timerTask.IsCanceled)
                return;
            TaskCompletionSource<Message> task = null;
            Message message = null;
            lock (_lock)
            {
                message = GetNextMessage();
                task = _task;
                _cancelRegistration.Dispose();
            }
            if (task != null && message != null)
                task.SetResult(message);
        }

        public void Put(Message message)
        {
            var guid = message.Headers.MessageId;
            TableProviderRow oldRow;
            if (message.Headers.MessageId == Guid.Empty || !_receivedMessages.TryGetValue(guid, out oldRow))
                PutNewMessage(message);
            else
                ReplaceOldMessage(message, oldRow);
        }

        private void ReplaceOldMessage(Message message, TableProviderRow oldRow)
        {
            var deliverOn = message.Headers.DeliverOn == DateTime.MinValue ? message.Headers.CreatedOn : message.Headers.DeliverOn;
            oldRow["deliveron"] = deliverOn.Ticks;
            oldRow["status"] = 0;
            oldRow["data"] = _serializer.Serialize(message);
            _table.Update(oldRow);
        }

        private void PutNewMessage(Message message)
        {
            if (message.Headers.MessageId == Guid.Empty)
                message.Headers.MessageId = Guid.NewGuid();
            if (message.Headers.CreatedOn == DateTime.MinValue)
                message.Headers.CreatedOn = _time.Get();
            var deliveron = message.Headers.DeliverOn == DateTime.MinValue ? message.Headers.CreatedOn : message.Headers.DeliverOn;
            var row = _table.NewRow();
            row["deliveron"] = deliveron.Ticks;
            row["status"] = 0;
            row["data"] = _serializer.Serialize(message);
            if (_queueName != null)
                row["queue"] = _queueName;
            _table.Insert(row);
        }
    }
}
