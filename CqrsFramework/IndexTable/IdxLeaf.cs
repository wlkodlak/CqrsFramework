using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public class IdxLeaf
    {
        private int _cellsCount = 0;
        private List<IdxCell> _cells = new List<IdxCell>();
        private bool _dirty = false;
        private int _next = 0;
        private int _size = 16;

        public IdxLeaf(byte[] bytesToLoad)
        {
            if (bytesToLoad != null)
                LoadFromBytes(bytesToLoad);
        }

        private void LoadFromBytes(byte[] bytesToLoad)
        {
            using (var reader = new BinaryReader(new MemoryStream(bytesToLoad)))
            {
                var nodeType = reader.ReadByte();
                _cellsCount = reader.ReadByte();
                reader.ReadBytes(2);
                _next = reader.ReadInt32();
                reader.ReadBytes(8);
                for (int i = 0; i < _cellsCount; i++)
                    _cells.Add(IdxCell.LoadLeafCell(reader));
            }
        }

        public int PageNumber { get; set; }
        public int CellsCount { get { return _cellsCount; } }
        public bool IsSmall { get { return _size < PagedFile.PageSize / 4; } }
        public bool IsFull { get { return _size + 128 > PagedFile.PageSize; } }
        public bool IsDirty { get { return _dirty; } }

        public int NextLeaf
        {
            get { return _next; }
            set
            {
                _dirty = true;
                _next = value;
            }
        }

        public void AddCell(IdxCell cell)
        {
            int positionToAdd = _cellsCount;
            for (int i = 0; i < _cellsCount; i++)
            {
                if (cell.Key < _cells[i].Key)
                {
                    positionToAdd = i;
                    break;
                }
            }
            _cells.Insert(positionToAdd, cell);
            _cellsCount++;
            _size += cell.CellSize;
            _dirty = true;
            for (int i = 0; i < _cellsCount; i++)
                _cells[i].Ordinal = i;
        }

        public IdxCell GetCell(int index)
        {
            return _cells[index];
        }

        public byte[] Save()
        {
            _dirty = false;
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write((byte)1);
                writer.Write((byte)_cellsCount);
                writer.Write((byte)0);
                writer.Write((byte)0);
                writer.Write(_next);
                writer.Write(new byte[8]);
                for (int i = 0; i < _cellsCount; i++)
                    _cells[i].SaveLeafCell(writer);
            }
            return buffer;
        }

        public void RemoveCell(int index)
        {
            var cell = _cells[index];
            _cellsCount--;
            _size -= cell.CellSize;
            _cells.RemoveAt(index);
            _dirty = true;
        }

        public IdxKey Split(IdxLeaf rightNode, IdxCell cell)
        {
            int positionToAdd = _cellsCount;
            for (int i = 0; i < _cellsCount; i++)
            {
                if (cell.Key < _cells[i].Key)
                {
                    positionToAdd = i;
                    break;
                }
            }
            var combinedCells = new List<IdxCell>(_cells);
            combinedCells.Insert(positionToAdd, cell);
            var totalSize = combinedCells.Sum(c => c.CellSize);
            var boundarySize = totalSize / 2;
            int leftCount = 0;
            int leftSize = 0;
            foreach (var measuredCell in combinedCells)
            {
                if (leftSize < boundarySize)
                {
                    leftSize += measuredCell.CellSize;
                    leftCount++;
                }
            }
            _cells = combinedCells.Take(leftCount).ToList();
            _cellsCount = _cells.Count;
            _size = leftSize + 16;
            _dirty = true;
            for (int i = 0; i < _cellsCount; i++)
                _cells[i].Ordinal = i;

            rightNode._cells = combinedCells.Skip(leftCount).ToList();
            rightNode._cellsCount = rightNode._cells.Count;
            rightNode._size = totalSize - leftSize + 16;
            rightNode._dirty = true;
            for (int i = 0; i < rightNode._cellsCount; i++)
                rightNode._cells[i].Ordinal = i;

            rightNode._next = _next;
            _next = rightNode.PageNumber;

            return rightNode._cells[0].Key;
        }

        public IdxKey Merge(IdxLeaf rightLeaf)
        {
            var key = rightLeaf.GetCell(0).Key;
            _next = rightLeaf._next;
            _cells.AddRange(rightLeaf._cells);
            _cellsCount += rightLeaf._cellsCount;
            _size += rightLeaf._size - 16;
            for (int i = 0; i < _cellsCount; i++)
                _cells[i].Ordinal = i;
            return key;
        }
    }
}
