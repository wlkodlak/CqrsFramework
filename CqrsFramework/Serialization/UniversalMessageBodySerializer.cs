using CqrsFramework.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public class UniversalMessageBodySerializer : IMessageBodySerializer
    {
        private UniversalMessageBodySerializer _master;
        private Dictionary<string, IMessageBodySerializer> _serializers;

        public UniversalMessageBodySerializer()
        {
            _master = this;
            _serializers = new Dictionary<string, IMessageBodySerializer>();
        }

        private UniversalMessageBodySerializer(UniversalMessageBodySerializer master, string newOutputFormat)
        {
            _master = master;
            OutputFormat = newOutputFormat ?? _master.OutputFormat;
        }

        public void RegisterFormat(string format, IMessageBodySerializer serializer)
        {
            if (OutputFormat == null)
                OutputFormat = format;
            _master._serializers.Add(format, serializer);
        }

        public UniversalMessageBodySerializer CreateLinked(string outputFormat = null)
        {
            return new UniversalMessageBodySerializer(_master, outputFormat);
        }

        public string OutputFormat { get; set; }

        public byte[] Serialize(object payload, MessageHeaders headers)
        {
            headers.PayloadFormat = OutputFormat;
            return _master._serializers[OutputFormat].Serialize(payload, headers);
        }

        public object Deserialize(byte[] serialized, MessageHeaders headers)
        {
            var format = headers.PayloadFormat;
            var serializer = _master._serializers[format];
            headers.PayloadFormat = null;
            return serializer.Deserialize(serialized, headers);
        }
    }
}
