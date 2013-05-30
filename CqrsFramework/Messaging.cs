using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework
{
    public interface IMessageInboxWriter
    {
        void Put(Message message);
    }

    public interface IMessageInboxReader
    {
        void Ack(Message message);
        void Nack(Message message);
        Task<Message> ReceiveAsync();
    }

    public interface IMessagePublisher
    {
        void Publish(Message message);
    }

    public interface IMessageRouter
    {
        void Publish(Message message);
        void Subscribe(IMessageInboxWriter inbox, MessageSubscriptionFilter filter);
        void Unsubscribe(IMessageInboxWriter inbox, MessageSubscriptionFilter filter);
    }

    public class MessageSubscriptionFilter
    {
    }
}
