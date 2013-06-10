using CqrsFramework.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public interface IMessageSerializer
    {
        byte[] Serialize(Message message);
        Message Deserialize(byte[] message);
    }

    public interface IMessageBodySerializer
    {
        byte[] Serialize(object payload, MessageHeaders headers);
        object Deserialize(byte[] serialized, MessageHeaders headers);
    }

    public interface IMessageTypeResolver
    {
        Type GetType(string name);
        string GetName(Type type);
        void RegisterType(Type type, string name, params string[] tags);
        string[] GetTags(string name);
    }
}
