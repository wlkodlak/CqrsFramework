using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxKey
    {
        private byte[] _bytes;
        private bool _isMaxValue;

        private IdxKey(byte[] bytes, bool isMaxValue)
        {
            _bytes = bytes;
            _isMaxValue = isMaxValue;
        }

        public static IdxKey FromString(string s)
        {
            return new IdxKey(Encoding.ASCII.GetBytes(s), false);
        }

        public static IdxKey FromInteger(int i)
        {
            byte[] result = new byte[4];
            uint moved = (uint)i + 0x80000000;
            result[0] = (byte)((moved >> 24) & 0xff);
            result[1] = (byte)((moved >> 16) & 0xff);
            result[2] = (byte)((moved >> 8) & 0xff);
            result[3] = (byte)((moved >> 0) & 0xff);
            return new IdxKey(result, false);
        }

        public static IdxKey FromBytes(byte[] bytes)
        {
            return new IdxKey(bytes ?? new byte[0], false);
        }

        public byte[] ToBytes()
        {
            return _bytes;
        }

        public override bool Equals(object obj)
        {
            return Compare(this, obj as IdxKey) == 0;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 0;
                for (int i = 0; i < _bytes.Length; i++)
                    hash = ((hash << 7) | (hash >> 25)) ^ _bytes[i];
                return hash;
            }
        }

        public override string ToString()
        {
            if (_isMaxValue)
                return "MAX";
            var sb = new StringBuilder(_bytes.Length * 3);
            for (int i = 0; i < _bytes.Length; i++)
            {
                if ((i & 1) == 0 && i != 0)
                    sb.Append(" ");
                sb.AppendFormat("{0:x2}", _bytes[i]);
            }
            return sb.ToString();
        }

        public static int Compare(IdxKey a, IdxKey b)
        {
            int idx = 0;
            byte[] arrA = ReferenceEquals(a, null) ? new byte[0] : a._bytes;
            byte[] arrB = ReferenceEquals(b, null) ? new byte[0] : b._bytes;
            int lenA = arrA.Length;
            int lenB = arrB.Length;
            bool maxA = !ReferenceEquals(a, null) && a._isMaxValue;
            bool maxB = !ReferenceEquals(b, null) && b._isMaxValue;

            if (maxA)
                return maxB ? 0 : 1;
            else if (maxB)
                return -1;

            while (lenA > 0 && lenB > 0)
            {
                byte valA = arrA[idx];
                byte valB = arrB[idx];
                if (valA < valB)
                    return -1;
                else if (valA > valB)
                    return 1;
                idx++;
                lenA--;
                lenB--;
            }
            if (lenB > 0)
                return -1;
            else if (lenA > 0)
                return 1;
            else
                return 0;
        }

        public static bool operator <(IdxKey a, IdxKey b)
        {
            return Compare(a, b) < 0;
        }
        public static bool operator <=(IdxKey a, IdxKey b)
        {
            return Compare(a, b) <= 0;
        }
        public static bool operator ==(IdxKey a, IdxKey b)
        {
            return Compare(a, b) == 0;
        }
        public static bool operator !=(IdxKey a, IdxKey b)
        {
            return Compare(a, b) != 0;
        }
        public static bool operator >=(IdxKey a, IdxKey b)
        {
            return Compare(a, b) >= 0;
        }
        public static bool operator >(IdxKey a, IdxKey b)
        {
            return Compare(a, b) > 0;
        }

        public static IdxKey MinValue
        {
            get { return new IdxKey(new byte[0], false); }
        }
        public static IdxKey MaxValue
        {
            get { return new IdxKey(new byte[1], true); }
        }
    }
}
