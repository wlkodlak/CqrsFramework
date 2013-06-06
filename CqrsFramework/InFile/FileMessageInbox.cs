using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization;

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
            var name = CreateQueueName(message, _sequenceId);
            _sequenceId = _sequenceId >= 999 ? 0 : _sequenceId + 1;
            using (var stream = _directory.Open(name, FileMode.CreateNew))
            {
                var bytes = _serializer.Serialize(message);
                stream.Write(bytes, 0, bytes.Length);
            }
        }

        private static string CreateQueueName(Message message, int sequenceId)
        {
            var date = message.Headers.CreatedOn.ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
            var order = sequenceId.ToString("000");
            var id = message.Headers.MessageId.ToString("N");
            return string.Concat(date, order, ".", id, ".queuemessage");
        }
    }

    public class FileMessageInboxReader : IMessageInboxReader
    {
        private IStreamProvider _directory;
        private IMessageSerializer _serializer;
        private ITimeProvider _time;

        public FileMessageInboxReader(IStreamProvider directory, IMessageSerializer serializer, ITimeProvider time)
        {
            _directory = directory;
            _serializer = serializer;
            _time = time;
            EnumerateDirectory();
        }

        private void EnumerateDirectory()
        {
            _directory.GetStreams();
        }

        public void Delete(Message message)
        {
            throw new NotImplementedException();
        }

        public Task<Message> ReceiveAsync(System.Threading.CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public void Put(Message message)
        {
            throw new NotImplementedException();
        }
    }
}
