using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Text;

namespace CqrsFramework.Serialization
{
    public class JsonMessageBodySerializer : IMessageBodySerializer
    {
        private IMessageTypeResolver _resolver;
        private Encoding _encoding;

        public JsonMessageBodySerializer(IEnumerable<Type> knownTypes, IMessageTypeResolver resolver)
        {
            this._resolver = resolver;
            this._encoding = new UTF8Encoding(false);
        }

        public byte[] Serialize(object payload, MessageHeaders headers)
        {
            var type = payload.GetType();
            var typename = _resolver.GetName(type);
            if (typename == null)
                throw new InvalidOperationException(string.Format("Unknown type {0}", type.Name));
            var resultString = JsonSerializer.SerializeToString(payload, type);
            var resultBytes = _encoding.GetBytes(resultString);
            headers["PayloadLength"] = resultBytes.Length.ToString();
            headers["PayloadType"] = typename;
            return resultBytes;
        }

        public object Deserialize(byte[] serialized, MessageHeaders headers)
        {
            var typename = headers["PayloadType"];
            var type = _resolver.GetType(typename);
            var inputString = _encoding.GetString(serialized);
            return JsonSerializer.DeserializeFromString(inputString, type);
        }
    }
}
