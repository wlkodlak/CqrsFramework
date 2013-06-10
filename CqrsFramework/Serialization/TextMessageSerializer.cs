using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public class TextMessageSerializer : IMessageSerializer
    {
        private Encoding _encoding = new UTF8Encoding(false);
        private IMessageBodySerializer _serializer;

        public TextMessageSerializer(IMessageBodySerializer serializer)
        {
            _serializer = serializer;
        }

        public byte[] Serialize(Message message)
        {
            var headers = new MessageHeaders();
            headers.CopyFrom(message.Headers);
            var serializedPayload = _serializer.Serialize(message.Payload, headers);
            using (var stream = new MemoryStream())
            {
                var writer = new StringBuilder();
                foreach (var header in headers)
                {
                    writer.AppendFormat("{0}: {1}", header.Name, header.Value);
                    writer.AppendLine();
                }
                writer.AppendLine();
                var buffer = _encoding.GetBytes(writer.ToString());
                stream.Write(buffer, 0, buffer.Length);
                stream.Write(serializedPayload, 0, serializedPayload.Length);
                return stream.ToArray();
            }
        }

        public Message Deserialize(byte[] message)
        {
            using (var stream = new MemoryStream(message))
            {
                var headers = new MessageHeaders();
                string line;
                while (!string.IsNullOrEmpty(line = ReadLine(stream)))
                {
                    var header = ParseHeader(line);
                    headers[header.Name] = header.Value;
                }
                var buffer = ReadToEnd(stream);
                var payload = _serializer.Deserialize(buffer, headers);
                var result = new Message(payload);
                result.Headers.CopyFrom(headers);
                return result;
            }
        }

        private byte[] ReadToEnd(Stream stream)
        {
            using (var output = new MemoryStream())
            {
                var buffer = new byte[4096];
                int read;
                while ((read = stream.Read(buffer, 0, 4096)) > 0)
                    output.Write(buffer, 0, read);
                return output.ToArray();
            }
        }

        private string ReadLine(Stream stream)
        {
            var buffer = new MemoryStream();
            while (true)
            {
                var current = stream.ReadByte();
                if (current == -1 || current == '\n')
                    break;
                else if (current != '\r')
                    buffer.WriteByte((byte)current);
            }
            return _encoding.GetString(buffer.ToArray());
        }

        private MessageHeader ParseHeader(string line)
        {
            if (string.IsNullOrEmpty(line))
                return null;
            var parts = line.Split(new char[] { ':' }, 2);
            var name = parts[0].Trim();
            var value = parts.Length < 2 ? "" : parts[1].TrimStart(' ', '\t');
            return new MessageHeader(name, value, false);
        }
    }
}
