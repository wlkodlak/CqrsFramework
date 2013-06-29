using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public static class ByteArrayUtils
    {
        public static byte[] BinaryInt(int value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }
        public static int BinaryInt(byte[] bytes)
        {
            return (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
        }
        public static byte[] BinaryLong(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }
        public static long BinaryLong(byte[] bytes)
        {
            return
                ((long)bytes[0] << 56) |
                ((long)bytes[1] << 48) |
                ((long)bytes[2] << 40) |
                ((long)bytes[3] << 32) |
                ((long)bytes[4] << 24) |
                ((long)bytes[5] << 16) |
                ((long)bytes[6] << 8) |
                (long)bytes[7];
        }

        static ByteArrayUtils()
        {
            _fromHex = new byte[256];
            for (int i = 0; i <= 9; i++)
                _fromHex[i + 0x30] = (byte)i;
            for (int i = 10; i <= 15; i++)
            {
                _fromHex[i + 0x41 - 10] = (byte)i;
                _fromHex[i + 0x61 - 10] = (byte)i;
            }
        }

        private static byte[] _toHex = new byte[16] { 
            0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46 };
        private static byte[] _fromHex;

        public static byte[] HexInt(int value)
        {
            var raw = BinaryInt(value);
            var result = new byte[8];
            result[0] = _toHex[(raw[0] & 0xf0) >> 4];
            result[1] = _toHex[(raw[0] & 0x0f)];
            result[2] = _toHex[(raw[1] & 0xf0) >> 4];
            result[3] = _toHex[(raw[1] & 0x0f)];
            result[4] = _toHex[(raw[2] & 0xf0) >> 4];
            result[5] = _toHex[(raw[2] & 0x0f)];
            result[6] = _toHex[(raw[3] & 0xf0) >> 4];
            result[7] = _toHex[(raw[3] & 0x0f)];
            return result;
        }
        public static byte[] HexLong(long value)
        {
            var raw = BinaryLong(value);
            var result = new byte[16];
            result[0] = _toHex[(raw[0] & 0xf0) >> 4];
            result[1] = _toHex[(raw[0] & 0x0f)];
            result[2] = _toHex[(raw[1] & 0xf0) >> 4];
            result[3] = _toHex[(raw[1] & 0x0f)];
            result[4] = _toHex[(raw[2] & 0xf0) >> 4];
            result[5] = _toHex[(raw[2] & 0x0f)];
            result[6] = _toHex[(raw[3] & 0xf0) >> 4];
            result[7] = _toHex[(raw[3] & 0x0f)];
            result[8] = _toHex[(raw[4] & 0xf0) >> 4];
            result[9] = _toHex[(raw[4] & 0x0f)];
            result[10] = _toHex[(raw[5] & 0xf0) >> 4];
            result[11] = _toHex[(raw[5] & 0x0f)];
            result[12] = _toHex[(raw[6] & 0xf0) >> 4];
            result[13] = _toHex[(raw[6] & 0x0f)];
            result[14] = _toHex[(raw[7] & 0xf0) >> 4];
            result[15] = _toHex[(raw[7] & 0x0f)];
            return result;
        }
        public static int HexInt(byte[] bytes)
        {
            return
                _fromHex[bytes[0]] << 28 |
                _fromHex[bytes[1]] << 24 |
                _fromHex[bytes[2]] << 20 |
                _fromHex[bytes[3]] << 16 |
                _fromHex[bytes[4]] << 12 |
                _fromHex[bytes[5]] << 8 |
                _fromHex[bytes[6]] << 4 |
                _fromHex[bytes[7]];
        }
        public static long HexLong(byte[] bytes)
        {
            return
                ((long)_fromHex[bytes[0]]) << 60 |
                ((long)_fromHex[bytes[1]]) << 56 |
                ((long)_fromHex[bytes[2]]) << 52 |
                ((long)_fromHex[bytes[3]]) << 48 |
                ((long)_fromHex[bytes[4]]) << 44 |
                ((long)_fromHex[bytes[5]]) << 40 |
                ((long)_fromHex[bytes[6]]) << 36 |
                ((long)_fromHex[bytes[7]]) << 32 |
                ((long)_fromHex[bytes[8]]) << 28 |
                ((long)_fromHex[bytes[9]]) << 24 |
                ((long)_fromHex[bytes[10]]) << 20 |
                ((long)_fromHex[bytes[11]]) << 16 |
                ((long)_fromHex[bytes[12]]) << 12 |
                ((long)_fromHex[bytes[13]]) << 8 |
                ((long)_fromHex[bytes[14]]) << 4 |
                ((long)_fromHex[bytes[15]]);
        }

        public static byte[] TextInt(int value)
        {
            return Encoding.ASCII.GetBytes(value.ToString());
        }
        public static byte[] TextLong(long value)
        {
            return Encoding.ASCII.GetBytes(value.ToString());
        }
        public static int TextInt(byte[] bytes)
        {
            int result;
            var s = Encoding.ASCII.GetString(bytes);
            int.TryParse(s, out result);
            return result;
        }
        public static long TextLong(byte[] bytes)
        {
            long result;
            var s = Encoding.ASCII.GetString(bytes);
            long.TryParse(s, out result);
            return result;
        }

        private static Encoding _utf8 = new UTF8Encoding(false);
        public static string Utf8Text(byte[] bytes)
        {
            return _utf8.GetString(bytes);
        }
        public static byte[] Utf8Text(string str)
        {
            return _utf8.GetBytes(str);
        }
    }
}
