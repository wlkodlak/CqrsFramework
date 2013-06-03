using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using System.IO;

namespace CqrsFramework.ServiceBus
{
    public class JsonMessageSerializer : AbstractMessageSerializer
    {
        private Encoding _encoding = new UTF8Encoding(false);

        public JsonMessageSerializer(IEnumerable<Type> knownTypes)
        {
            Build(knownTypes);
        }

        protected override IFormatter CreateFormatter(Type type)
        {
            throw new NotImplementedException();
        }

        protected override void WriteTypeinfo(string typename, Stream stream)
        {
            using (var writer = new StreamWriter(stream, _encoding, 1024, true))
                writer.WriteLine("Typename: {0}", typename);
        }

        protected override void WriteHeadersCollection(Message message, Stream stream)
        {
            using (var writer = new StreamWriter(stream, _encoding, 1024, true))
            {
                foreach (var header in message.Headers)
                    writer.WriteLine("{0}: {1}", header.Name, header.Value);
                writer.WriteLine();
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

        protected override string ReadTypeinfo(Stream stream)
        {
            var wholeLine = ReadLine(stream);
            var header = ParseHeader(wholeLine);
            if (header.Name != "Typename")
                throw new FormatException();
            return header.Value;
        }

        protected override List<MessageHeader> ReadHeadersCollection(Stream stream)
        {
            var headers = new List<MessageHeader>();
            string line;
            while (!string.IsNullOrEmpty(line = ReadLine(stream)))
                headers.Add(ParseHeader(line));
            return headers;
        }
    }
}
