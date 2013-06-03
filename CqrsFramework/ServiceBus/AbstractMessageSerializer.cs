using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.ServiceBus
{
    public abstract class AbstractMessageSerializer : IMessageSerializer
    {
        private class Typeinfo
        {
            public string Name;
            public Type Type;
            public IFormatter Formatter;
        }

        private Dictionary<string, Typeinfo> _typeinfoByName = new Dictionary<string, Typeinfo>();
        private Dictionary<Type, Typeinfo> _typeinfoByType = new Dictionary<Type, Typeinfo>();

        public void Build(IEnumerable<Type> knownTypes)
        {
            foreach (var type in knownTypes)
            {
                var typeinfo = new Typeinfo();
                typeinfo.Type = type;
                typeinfo.Name = GetTypename(type);
                typeinfo.Formatter = CreateFormatter(type);
                _typeinfoByName[typeinfo.Name] = typeinfo;
                _typeinfoByType[typeinfo.Type] = typeinfo;
            }
        }

        protected virtual string GetTypename(Type type)
        {
            return type.FullName;
        }

        protected abstract IFormatter CreateFormatter(Type type);

        public byte[] Serialize(Message message)
        {
            using (var stream = new MemoryStream())
            {
                var typeinfo = _typeinfoByType[message.Payload.GetType()];
                WriteTypeinfo(typeinfo.Name, stream);
                WriteHeadersCollection(message, stream);
                typeinfo.Formatter.Serialize(stream, message.Payload);
                return stream.ToArray();
            }
        }

        public Message Deserialize(byte[] serialized)
        {
            using (var stream = new MemoryStream(serialized))
            {
                var typename = ReadTypeinfo(stream);
                var typeinfo = _typeinfoByName[typename];
                var headers = ReadHeadersCollection(stream);
                var payload = typeinfo.Formatter.Deserialize(stream);
                var message = new Message(payload);
                foreach (var header in headers)
                    message.Headers[header.Name] = header.Value;
                return message;
            }
        }

        protected abstract void WriteTypeinfo(string typename, Stream stream);
        protected abstract void WriteHeadersCollection(Message message, Stream stream);
        protected abstract string ReadTypeinfo(Stream stream);
        protected abstract List<MessageHeader> ReadHeadersCollection(Stream stream);
    }
}
