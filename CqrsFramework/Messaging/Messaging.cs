using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public interface IMessageDispatcher
    {
        void Dispatch(Message message);
    }

    public interface IMessagePublisher
    {
        void Publish(Message message);
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
}
