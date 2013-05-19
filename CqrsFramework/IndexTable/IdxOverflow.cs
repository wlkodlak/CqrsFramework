using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxOverflow : IdxPageBase
    {
        private int _length = 0;
        private int _next = 0;
        private bool _needsNext = false;
        private byte[] _data = null;

        public IdxOverflow(byte[] data)
        {
            if (data == null)
                return;
            using (var reader = new BinaryReader(new MemoryStream(data), Encoding.ASCII, false))
            {
                _next = reader.ReadInt32();
                _length = reader.ReadInt16();
                byte flags = reader.ReadByte();
                _needsNext = flags == 1;
                reader.ReadByte();
                _data = reader.ReadBytes(_length);
            }
        }

        public byte[] Save()
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer), Encoding.ASCII, false))
            {
                writer.Write(_next);
                writer.Write((short)_length);
                writer.Write(_needsNext ? (byte)1 : (byte)0);
                writer.Write((byte)0);
                writer.Write(_data);
            }
            SetDirty(false);
            return buffer;
        }

        public static int Capacity { get { return PagedFile.PageSize - 8; } }

        public int Next
        {
            get { return _next; }
            set
            {
                _next = value;
                SetDirty(true);
            }
        }
        public int LengthInPage { get { return _length; } }
        public bool HasNextPage { get { return _next != 0; } }
        public bool NeedsNextPage { get { return _needsNext; } }

        public int WriteData(byte[] buffer, int offset)
        {
            int remainingLength = buffer.Length - offset;
            if (remainingLength <= Capacity)
            {
                _length = remainingLength;
                _needsNext = false;
            }
            else
            {
                _length = Capacity;
                _needsNext = true;
            }
            _data = new byte[_length];
            Array.Copy(buffer, offset, _data, 0, _length);
            SetDirty(true);
            return _length;
        }

        public int ReadData(byte[] buffer, int offset)
        {
            Array.Copy(_data, 0, buffer, offset, _length);
            return _length;
        }
    }
}
