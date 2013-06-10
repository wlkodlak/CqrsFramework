using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using ProtoBuf;
using System.Runtime.Serialization;

namespace CqrsFramework.Serialization
{
    public class ProtobufMessageBodySerializer : IMessageBodySerializer
    {
        private IMessageTypeResolver _resolver;
        private Dictionary<string, SerializerInfo> _byName = new Dictionary<string, SerializerInfo>(StringComparer.Ordinal);
        private Dictionary<Type, SerializerInfo> _byType = new Dictionary<Type, SerializerInfo>();

        private class SerializerInfo
        {
            public string Name;
            public IFormatter Formatter;
        }

        public ProtobufMessageBodySerializer(IEnumerable<Type> knownTypes, IMessageTypeResolver resolver)
        {
            this._resolver = resolver;
            Func<IFormatter> objectCreator = Serializer.CreateFormatter<object>;
            var template = objectCreator.Method.GetGenericMethodDefinition();
            foreach (var type in knownTypes)
            {
                var name = _resolver.GetName(type);
                var formatter = template.MakeGenericMethod(type).Invoke(null, null) as IFormatter;
                var info = new SerializerInfo { Name = name, Formatter = formatter };
                _byName[info.Name] = info;
                _byType[type] = info;
            }
        }

        public byte[] Serialize(object payload, MessageHeaders headers)
        {
            using (var stream = new MemoryStream())
            {
                SerializerInfo info = GetFormatter(payload.GetType());
                info.Formatter.Serialize(stream, payload);
                var bytes = stream.ToArray();
                headers.PayloadLength = bytes.Length;
                headers.PayloadType = info.Name;
                return bytes;
            }
        }

        private SerializerInfo GetFormatter(Type type)
        {
            SerializerInfo info;
            if (!_byType.TryGetValue(type, out info))
                throw new InvalidOperationException(string.Format("Unknown type {0}", type.Name));
            return info;
        }

        public object Deserialize(byte[] serialized, MessageHeaders headers)
        {
            SerializerInfo info;
            var typename = headers.PayloadType;
            if (!_byName.TryGetValue(typename, out info))
                throw new InvalidOperationException(string.Format("Unknown typename {0}", typename));
            headers.PayloadLength = 0;
            headers.PayloadType = null;
            using (var stream = new MemoryStream(serialized))
                return info.Formatter.Deserialize(stream);
        }
    }
}
