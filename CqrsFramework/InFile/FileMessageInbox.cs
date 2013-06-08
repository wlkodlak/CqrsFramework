using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization;
using System.Threading;

namespace CqrsFramework.InFile
{
    public class FileMessageInboxWriter : IMessageInboxWriter
    {
        private IStreamProvider _directory;
        private IMessageSerializer _serializer;
        private ITimeProvider _time;
        private int _sequenceId = 0;

        public FileMessageInboxWriter(IStreamProvider directory, IMessageSerializer serializer, ITimeProvider time)
        {
            _directory = directory;
            _serializer = serializer;
            _time = time;
        }

        public void Put(Message message)
        {
            if (message.Headers.CreatedOn == DateTime.MinValue)
                message.Headers.CreatedOn = _time.Get();
            if (message.Headers.MessageId == Guid.Empty)
                message.Headers.MessageId = Guid.NewGuid();
            var name = FileMessageInboxReader.CreateQueueName(message, _sequenceId);
            _sequenceId = _sequenceId >= 999 ? 0 : _sequenceId + 1;
            using (var stream = _directory.Open(name, FileMode.CreateNew))
            {
                var bytes = _serializer.Serialize(message);
                stream.Write(bytes, 0, bytes.Length);
            }
        }

    }

    public class FileMessageInboxReader : IMessageInboxReader
    {
        private IStreamProvider _directory;
        private IMessageSerializer _serializer;
        private ITimeProvider _time;
        private TaskCompletionSource<Message> _task;
        private object _lock = new object();
        private readonly TimeSpan _checkInterval = TimeSpan.FromMilliseconds(100);
        private Task _timerTask;
        private CancellationTokenRegistration _cancelRegistration;
        private HashSet<string> _unconfirmedStreamNames;
        private Dictionary<Guid, string> _unconfirmedMessages;
        private FileMessageInboxWriter _inboxWriter;
        private Queue<string> _cachedStreamNames;

        private string GetNextStreamName()
        {
            if (_cachedStreamNames.Count > 0)
                return _cachedStreamNames.Dequeue();

            var streams = _directory.GetStreams();
            foreach (var streamName in streams)
            {
                if (!streamName.EndsWith(".queuemessage"))
                    continue;
                if (_unconfirmedStreamNames.Contains(streamName))
                    continue;
                _cachedStreamNames.Enqueue(streamName);
            }

            if (_cachedStreamNames.Count > 0)
                return _cachedStreamNames.Dequeue();
            return null;
        }

        public static string CreateQueueName(Message message, int sequenceId)
        {
            var date = message.Headers.CreatedOn.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
            var order = sequenceId.ToString("000");
            var id = message.Headers.MessageId.ToString("N");
            return string.Concat(date, order, ".", id, ".queuemessage");
        }

        public FileMessageInboxReader(IStreamProvider directory, IMessageSerializer serializer, ITimeProvider time)
        {
            _directory = directory;
            _serializer = serializer;
            _time = time;
            _unconfirmedStreamNames = new HashSet<string>();
            _unconfirmedMessages = new Dictionary<Guid, string>();
            _cachedStreamNames = new Queue<string>();
            _inboxWriter = new FileMessageInboxWriter(_directory, _serializer, _time);
        }

        public void Delete(Message message)
        {
            lock (_lock)
            {
                var messageId = message.Headers.MessageId;
                string streamName;
                if (_unconfirmedMessages.TryGetValue(messageId, out streamName))
                {
                    _directory.Delete(streamName);
                    _unconfirmedMessages.Remove(messageId);
                    _unconfirmedStreamNames.Remove(streamName);
                }
            }
        }

        public Task<Message> ReceiveAsync(CancellationToken token)
        {
            lock (_lock)
            {
                _task = new TaskCompletionSource<Message>();
                var nextStream = GetNextStreamName();
                if (nextStream == null)
                {
                    _cancelRegistration = token.Register(CancelHandler);
                    _timerTask = _time.WaitUntil(_time.Get().Add(_checkInterval), token);
                    _timerTask.ContinueWith(TimerHandler, token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Current);
                }
                else
                {
                    _unconfirmedStreamNames.Add(nextStream);
                    var message = ReadMessage(nextStream);
                    _unconfirmedMessages[message.Headers.MessageId] = nextStream;
                    _task.SetResult(message);
                }
                return _task.Task;
            }
        }

        private void CancelHandler()
        {
            TaskCompletionSource<Message> task = null;
            lock (_lock)
            {
                task = _task;
                _task = null;
            }
            if (task != null)
                task.TrySetCanceled();
        }

        private void TimerHandler(Task timerTask)
        {
            TaskCompletionSource<Message> task = null;
            Message message = null;
            lock (_lock)
            {
                var nextStream = GetNextStreamName();
                message = ReadMessage(nextStream);
                _cancelRegistration.Dispose();
                task = _task;
                _unconfirmedMessages[message.Headers.MessageId] = nextStream;
                _task = null;
            }
            if (task != null && message != null)
                task.SetResult(message);
        }

        private Message ReadMessage(string streamName)
        {
            using (var input = _directory.Open(streamName, FileMode.Open))
            using (var allBytes = new MemoryStream())
            {
                var buffer = new byte[4096];
                int readBytes;
                while ((readBytes = input.Read(buffer, 0, 4096)) > 0)
                    allBytes.Write(buffer, 0, readBytes);
                return _serializer.Deserialize(allBytes.ToArray());
            }
        }

        public void Put(Message message)
        {
            lock (_lock)
            {
                string streamName;
                var guid = message.Headers.MessageId;
                if (guid == Guid.Empty || !_unconfirmedMessages.TryGetValue(guid, out streamName))
                    _inboxWriter.Put(message);
                else
                {
                    _unconfirmedMessages.Remove(guid);
                    _unconfirmedStreamNames.Remove(streamName);
                    var serialized = _serializer.Serialize(message);
                    using (var stream = _directory.Open(streamName, FileMode.Create))
                        stream.Write(serialized, 0, serialized.Length);
                }
            }
        }
    }
}
