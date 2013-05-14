using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxCell
    {
        private int _keyLength;
        private IdxKey _key;
        private int _valueLength;
        private byte[] _value;
        private int _overflowLength;
        private int _overflowPage;
        private int _ordinal;
        private int _childPage;
        private bool _isLeaf;

        private const int MinSize = PagedFile.PageSize / 256;

        private IdxCell()
        {
        }

        public static IdxCell CreateLeafCell(IdxKey key, byte[] cellData)
        {
            var cell = new IdxCell();
            cell._isLeaf = true;
            cell._key = key;
            cell._keyLength = key.ToBytes().Length;
            int spaceForValue = 120 - cell._keyLength;
            if (spaceForValue < 0)
                throw new ArgumentOutOfRangeException("Key is too long");
            if (cellData == null)
            {
                cell._valueLength = 0;
                cell._value = null;
                cell._overflowLength = 0;
            }
            else
            {
                cell._valueLength = Math.Min(spaceForValue, cellData.Length);
                cell._value = cellData.Take(cell._valueLength).ToArray();
                cell._overflowLength = (cellData.Length - cell._valueLength + IdxOverflow.Capacity - 1) / IdxOverflow.Capacity;
            }
            return cell;
        }

        public bool IsLeaf { get { return _isLeaf; } }
        public int KeyLength { get { return _keyLength; } }
        public IdxKey Key { get { return _key; } }
        public int ValueLength { get { return _valueLength; } }
        public int OverflowLength { get { return _overflowLength; } }
        public int OverflowPage
        {
            get { return _overflowPage; }
            set { _overflowPage = value; }
        }
        public int ChildPage
        {
            get { return _childPage; }
            set { _childPage = value; }
        }
        public byte[] ValueBytes { get { return _value; } }
        public int CellSize { get { return Math.Max(MinSize, 8 + _keyLength + _valueLength); } }

        public static IdxCell LoadLeafCell(BinaryReader reader)
        {
            var cell = new IdxCell();
            cell._isLeaf = true;
            cell._keyLength = reader.ReadByte();
            cell._valueLength = reader.ReadByte();
            cell._overflowLength = reader.ReadInt16();
            cell._overflowPage = reader.ReadInt32();
            cell._key = IdxKey.FromBytes(reader.ReadBytes(cell._keyLength));
            cell._value = reader.ReadBytes(cell._valueLength);
            int remainingBytes = 8 - Math.Min(8, cell._keyLength + cell._valueLength);
            if (remainingBytes > 0)
                reader.ReadBytes(remainingBytes);
            return cell;
        }

        public void SaveLeafCell(BinaryWriter writer)
        {
            writer.Write((byte)_keyLength);
            writer.Write((byte)_valueLength);
            writer.Write((short)_overflowLength);
            writer.Write(_overflowPage);
            writer.Write(_key.ToBytes());
            if (_valueLength > 0)
                writer.Write(_value);
            int remainingBytes = 8 - Math.Min(8, _keyLength + _valueLength);
            if (remainingBytes > 0)
                writer.Write(new byte[remainingBytes]);
        }

        public int Ordinal
        {
            get { return _ordinal; }
            set { _ordinal = value; }
        }

        public static IdxCell CreateInteriorCell(IdxKey idxKey, int page)
        {
            var cell = new IdxCell();
            cell._key = idxKey;
            cell._keyLength = idxKey.ToBytes().Length;
            cell._childPage = page;
            cell._isLeaf = false;
            return cell;
        }

        public static IdxCell LoadInteriorCell(BinaryReader reader)
        {
            var cell = new IdxCell();
            cell._isLeaf = false;
            cell._keyLength = reader.ReadByte();
            reader.ReadBytes(3);
            cell._childPage = reader.ReadInt32();
            cell._key = IdxKey.FromBytes(reader.ReadBytes(cell._keyLength));
            if (cell._keyLength < 8)
                reader.ReadBytes(8 - cell._keyLength);
            return cell;
        }

        public void SaveInteriorCell(BinaryWriter writer)
        {
            writer.Write((byte)_keyLength);
            writer.Write(new byte[3]);
            writer.Write(_childPage);
            writer.Write(_key.ToBytes());
            if (_keyLength < 8)
                writer.Write(new byte[8 - _keyLength]);
        }
    }
}
