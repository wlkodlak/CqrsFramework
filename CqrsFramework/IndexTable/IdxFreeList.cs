using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxFreeList : IdxPageBase
    {
        public const int Capacity = PagedFile.PageSize / 4 - 2;
        private int _next;
        private int _length;
        private int[] _freePages;

        public IdxFreeList(byte[] data)
        {
            if (data == null)
            {
                _next = 0;
                _freePages = new int[Capacity];
                _length = 0;
                SetDirty(false);
            }
            else
            {
                using (var reader = new BinaryReader(new MemoryStream(data), Encoding.ASCII, false))
                {
                    _next = reader.ReadInt32();
                    _length = reader.ReadInt32();
                    _freePages = new int[Capacity];
                    SetDirty(false);
                    for (int i = 0; i < _length; i++)
                        _freePages[i] = reader.ReadInt32();
                }
            }
        }

        public int Next
        {
            get { return _next; }
            set
            {
                _next = value;
                SetDirty(true);
            }
        }

        public int Length { get { return _length; } }
        public bool IsFull { get { return _length == Capacity; } }
        public bool IsEmpty { get { return _length == 0; } }
        public bool IsLast { get { return _next == 0; } }

        public void Add(int page)
        {
            _freePages[_length] = page;
            _length++;
            SetDirty(true);
        }

        public override byte[] Save()
        {
            var data = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(data), Encoding.ASCII, false))
            {
                writer.Write(_next);
                writer.Write(_length);
                foreach (int page in _freePages)
                    writer.Write(page);
            }
            SetDirty(false);
            return data;
        }

        public int Alloc()
        {
            if (_length == 0)
                throw new InvalidOperationException();
            _length--;
            int allocated = _freePages[_length];
            _freePages[_length] = 0;
            SetDirty(true);
            return allocated;
        }
    }
}
