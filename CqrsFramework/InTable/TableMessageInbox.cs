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
        private IMessageSerializer _serializer;
        private ITimeProvider _time;
        private object _lock = new object();
        private TaskCompletionSource<Message> _task;
        private CancellationTokenRegistration _cancelRegistration;
        private readonly TimeSpan _checkInterval = TimeSpan.FromMilliseconds(100);
        private Task _timerTask;
        private Dictionary<Guid, TableProviderRow> _receivedMessages = new Dictionary<Guid, TableProviderRow>();

        public TableMessageInbox(ITableProvider table, IMessageSerializer serializer, ITimeProvider time)
        {
            _table = table;
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
                var message = GetNextMessage(true);
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

        private Message GetNextMessage(bool fromAll)
        {
            var filterable = _table.GetRows();
            if (!fromAll)
                filterable = filterable.Where("status").Is(0);
            var row = filterable.FirstOrDefault();
            if (row != null)
            {
                row["status"] = 2;
                _table.Update(row);
            }
            var message = MessageFromRow(row);
            if (message != null)
                _receivedMessages[message.Headers.MessageId] = row;
            return message;
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
                message = GetNextMessage(false);
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
            var newRow = _table.NewRow();
            newRow[1] = deliverOn;
            newRow[2] = 1;
            newRow[3] = _serializer.Serialize(message);
            _table.Insert(newRow);
            _table.Delete(oldRow);
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
            _table.Insert(row);
        }
    }
}
