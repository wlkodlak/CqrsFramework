using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework
{
    public interface IMessageDispatcher
    {
        void Dispatch(Message message);
    }

    public interface IMessagePublisher
    {
        void Publish(Message message);
    }

    public interface ITimeProvider
    {
        DateTime Get();
        Task WaitUntil(DateTime time, CancellationToken cancel);
    }

    public class RealTimeProvider : ITimeProvider
    {
        public DateTime Get()
        {
            return DateTime.UtcNow;
        }

        public Task WaitUntil(DateTime time, CancellationToken cancel)
        {
            return Task.Delay(time - Get(), cancel);
        }
    }

    public interface IMessageInboxWriter
    {
        void Put(Message message);
    }

    public interface IMessageInboxReader : IMessageInboxWriter
    {
        void Delete(Message message);
        Task<Message> ReceiveAsync(CancellationToken token);
    }

    public interface IMessageSerializer
    {
        byte[] Serialize(Message message);
        Message Deserialize(byte[] message);
    }
}
