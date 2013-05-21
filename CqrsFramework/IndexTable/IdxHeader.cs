using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public class IdxHeader
    {
        private readonly static byte[] MagicHeader = new byte[4] { 0x49, 0x58, 0x54, 0x4c };

        private int _freePagesList = 0;
        private int _totalPages = 0;
        private int[] _trees = new int[16];
        private bool _dirty = false;

        public IdxHeader(byte[] data)
        {
            LoadFromBytes(data);
        }

        private void LoadFromBytes(byte[] data)
        {
            if (data == null)
                return;
            using (var reader = new BinaryReader(new MemoryStream(data)))
            {
                var magic = reader.ReadBytes(4);
                VerifyMagicHeader(magic);
                _freePagesList = reader.ReadInt32();
                _totalPages = reader.ReadInt32();
                reader.ReadBytes(20);
                for (int i = 0; i < 16; i++)
                    _trees[i] = reader.ReadInt32();
            }
        }

        private static void VerifyMagicHeader(byte[] magic)
        {
            var magicIsValid = true;
            for (int i = 0; i < MagicHeader.Length; i++)
                magicIsValid = magicIsValid && magic[i] == MagicHeader[i];
            if (!magicIsValid)
                throw new InvalidDataException("Index table header invalid");
        }

        public int FreePagesList
        {
            get { return _freePagesList; }
            set
            {
                _freePagesList = value;
                _dirty = true;
            }
        }

        public int TotalPagesCount
        {
            get { return _totalPages; }
            set
            {
                _totalPages = value;
                _dirty = true;
            }
        }

        public int GetTreeRoot(int treeNumber)
        {
            return _trees[treeNumber];
        }

        public void SetTreeRoot(int treeNumber, int page)
        {
            _trees[treeNumber] = page;
            _dirty = true;
        }

        public bool IsDirty
        {
            get { return _dirty; }
        }

        public byte[] Save()
        {
            _dirty = false;
            var buffer = new byte[IdxPagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(MagicHeader);
                writer.Write(_freePagesList);
                writer.Write(_totalPages);
                writer.Write(new byte[20]); // reserved
                for (int i = 0; i < 16; i++)
                    writer.Write(_trees[i]);
            }
            return buffer;
        }
    }
}
