using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using ProtoBuf;
using System.Runtime.Serialization;

namespace CqrsFramework.ServiceBus
{
    public class ProtobufMessageSerializer : AbstractMessageSerializer
    {
        System.Reflection.MethodInfo _createFormatterGeneric;

        public ProtobufMessageSerializer(IEnumerable<Type> knownTypes)
        {
            Func<IFormatter> templateMethod = Serializer.CreateFormatter<object>;
            _createFormatterGeneric = templateMethod.Method.GetGenericMethodDefinition();
            Build(knownTypes);
        }

        protected override IFormatter CreateFormatter(Type type)
        {
            return (IFormatter)_createFormatterGeneric.MakeGenericMethod(type).Invoke(null, null);
        }

        protected override void WriteTypeinfo(string typename, Stream stream)
        {
            byte[] nameBytes = Encoding.ASCII.GetBytes(typename);
            stream.WriteByte((byte)nameBytes.Length);
            stream.Write(nameBytes, 0, nameBytes.Length);
        }

        protected override void WriteHeadersCollection(Message message, Stream stream)
        {
            var headers = message.Headers.Take(255).ToList();
            stream.WriteByte((byte)headers.Count);
            foreach (var header in headers)
                WriteSingleHeader(stream, header);
        }

        private void WriteSingleHeader(Stream stream, MessageHeader header)
        {
            byte[] nameBytes = Encoding.ASCII.GetBytes(header.Name);
            byte[] valueBytes = Encoding.UTF8.GetBytes(header.Value);
            stream.WriteByte((byte)nameBytes.Length);
            stream.WriteByte((byte)valueBytes.Length);
            stream.Write(nameBytes, 0, nameBytes.Length);
            stream.Write(valueBytes, 0, valueBytes.Length);
        }

        protected override string ReadTypeinfo(Stream stream)
        {
            var length = stream.ReadByte();
            var nameBytes = new byte[length];
            stream.Read(nameBytes, 0, length);
            return Encoding.ASCII.GetString(nameBytes);
        }

        protected override List<MessageHeader> ReadHeadersCollection(Stream stream)
        {
            var count = stream.ReadByte();
            var list = new List<MessageHeader>(count);
            for (int i = 0; i < count; i++)
                list.Add(ReadSingleHeader(stream));
            return list;
        }

        private MessageHeader ReadSingleHeader(Stream stream)
        {
            var nameLength = stream.ReadByte();
            var valueLength = stream.ReadByte();
            byte[] nameBytes = new byte[nameLength];
            byte[] valueBytes = new byte[valueLength];
            stream.Read(nameBytes, 0, nameLength);
            stream.Read(valueBytes, 0, valueLength);
            return new MessageHeader(Encoding.ASCII.GetString(nameBytes), Encoding.UTF8.GetString(valueBytes), false);
        }

    }
}
