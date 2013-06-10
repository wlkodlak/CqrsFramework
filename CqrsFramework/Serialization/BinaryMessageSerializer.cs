using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public class BinaryMessageSerializer : IMessageSerializer
    {
        private Encoding _encoding;
        private IMessageBodySerializer _serializer;

        public BinaryMessageSerializer(IMessageBodySerializer serializer)
        {
            _serializer = serializer;
            _encoding = new UTF8Encoding(false);
        }

        public byte[] Serialize(Message message)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    var headers = new MessageHeaders();
                    headers.CopyFrom(message.Headers);
                    var serializedPayload = _serializer.Serialize(message.Payload, headers);

                    foreach (var header in headers)
                    {
                        var nameBytes = _encoding.GetBytes(header.Name);
                        var valueBytes = _encoding.GetBytes(header.Value);
                        writer.Write((byte)nameBytes.Length);
                        writer.Write(nameBytes);
                        writer.Write((byte)valueBytes.Length);
                        writer.Write(valueBytes);
                    }
                    writer.Write((byte)0);
                    writer.Write(serializedPayload);
                }
                return stream.ToArray();
            }
        }

        public Message Deserialize(byte[] message)
        {
            using (var stream = new MemoryStream(message))
            {
                using (var reader = new BinaryReader(stream))
                {
                    var headers = new MessageHeaders();
                    while (ReadHeader(reader, headers));
                    var buffer = ReadToEnd(reader);
                    var payload = _serializer.Deserialize(buffer, headers);
                    var result = new Message(payload);
                    result.Headers.CopyFrom(headers);
                    return result;
                }
            }
        }

        private bool ReadHeader(BinaryReader reader, MessageHeaders headers)
        {
            var nameLength = reader.ReadByte();
            if (nameLength == 0)
                return false;
            var name = _encoding.GetString(reader.ReadBytes(nameLength));
            var valueLength = reader.ReadByte();
            var value = _encoding.GetString(reader.ReadBytes(valueLength));
            headers[name] = value;
            return true;
        }

        private byte[] ReadToEnd(BinaryReader reader)
        {
            using (var stream = new MemoryStream())
            {
                var buffer = new byte[4096];
                int read;
                while ((read = reader.Read(buffer, 0, 4096)) > 0)
                    stream.Write(buffer, 0, read);
                return stream.ToArray();
            }
        }
    }
}
