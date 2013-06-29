using CqrsFramework.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public interface IKeyValueProjectionStrategy
    {
        byte[] SerializeClock(long clock);
        long DeserializeClock(byte[] bytes);
        string GetTypename(Type type);
        byte[] SerializeView(Type type, object view);
        object DeserializeView(Type type, byte[] bytes);
    }

    public enum KeyValueProjectionClockFormat
    {
        Binary,
        Hex,
        Text
    }

    public class KeyValueProjectionStrategy : IKeyValueProjectionStrategy
    {
        private IMessageBodySerializer _serializer;
        private string _typename;
        private Func<byte[], long> _clockDeserialize;
        private Func<long, byte[]> _clockSerialize;

        public KeyValueProjectionStrategy(IMessageBodySerializer serializer, string typename, KeyValueProjectionClockFormat clockFormat)
        {
            _serializer = serializer;
            _typename = typename;
            switch (clockFormat)
            {
                case KeyValueProjectionClockFormat.Binary:
                    _clockDeserialize = ByteArrayUtils.BinaryLong;
                    _clockSerialize = ByteArrayUtils.BinaryLong;
                    break;
                case KeyValueProjectionClockFormat.Hex:
                    _clockDeserialize = ByteArrayUtils.HexLong;
                    _clockSerialize = ByteArrayUtils.HexLong;
                    break;
                case KeyValueProjectionClockFormat.Text:
                    _clockDeserialize = ByteArrayUtils.TextLong;
                    _clockSerialize = ByteArrayUtils.TextLong;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("clockFormat");
            }
        }


        public string GetTypename(Type type)
        {
            return _typename;
        }

        public byte[] SerializeView(Type type, object view)
        {
            var headers = new MessageHeaders();
            return _serializer.Serialize(view, headers);
        }

        public object DeserializeView(Type type, byte[] bytes)
        {
            var headers = new MessageHeaders();
            headers.PayloadType = GetTypename(type);
            return _serializer.Deserialize(bytes, headers);
        }

        public byte[] SerializeClock(long clock)
        {
            return _clockSerialize(clock);
        }

        public long DeserializeClock(byte[] bytes)
        {
            return _clockDeserialize(bytes);
        }
    }
}
