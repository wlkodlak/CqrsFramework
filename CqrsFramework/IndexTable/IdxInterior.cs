using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxInterior
    {
        private List<IdxCell> _cells = new List<IdxCell>();
        private int _cellsCount = 0;
        private int _size = 16;
        private bool _dirty;
        private int _leftMost = 0;

        public IdxInterior(byte[] bytes)
        {
            if (bytes != null)
                LoadBytes(bytes);
        }

        private void LoadBytes(byte[] bytes)
        {
            using (var reader = new BinaryReader(new MemoryStream(bytes)))
            {
                var nodeType = reader.ReadByte();
                _cellsCount = reader.ReadByte();
                reader.ReadBytes(2);
                _leftMost = reader.ReadInt32();
                reader.ReadBytes(8);
                for (int i = 0; i < _cellsCount; i++)
                {
                    var cell = IdxCell.LoadInteriorCell(reader);
                    cell.Ordinal = i;
                    _cells.Add(cell);
                    _size += cell.CellSize;
                }
            }
        }

        public int LeftmostPage
        {
            get { return _leftMost; }
            set
            {
                _leftMost = value;
                _dirty = true;
            }
        }

        public int PageNumber { get; set; }
        public int CellsCount { get { return _cellsCount; } }
        public bool IsSmall { get { return _size < PagedFile.PageSize / 4; } }
        public bool IsFull { get { return _size + 128 > PagedFile.PageSize; } }
        public bool IsDirty { get { return _dirty; } }

        public void AddCell(IdxCell cell)
        {
            int position = InsertPosition(cell);
            _cells.Insert(position, cell);
            _cellsCount++;
            _size += cell.CellSize;
            _dirty = true;
            FixOrdinals();
        }

        private int InsertPosition(IdxCell cell)
        {
            for (int i = 0; i < _cells.Count; i++)
            {
                if (cell.Key < _cells[i].Key)
                    return i;
            }
            return _cellsCount;
        }

        private void FixOrdinals()
        {
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
                writer.Write((byte)2);
                writer.Write((byte)_cellsCount);
                writer.Write((byte)0);
                writer.Write((byte)0);
                writer.Write(_leftMost);
                writer.Write(new byte[8]);
                foreach (var cell in _cells)
                    cell.SaveInteriorCell(writer);
            }
            return buffer;
        }

        public void RemoveCell(int index)
        {
            var cell = _cells[index];
            _size -= cell.CellSize;
            _cellsCount--;
            _dirty = true;
            _cells.RemoveAt(index);
            FixOrdinals();
        }

        public int FindPage(IdxKey key)
        {
            int resultPage = _leftMost;
            foreach (var cell in _cells)
                if (key >= cell.Key)
                    resultPage = cell.ChildPage;
            return resultPage;
        }

        public IdxKey Split(IdxInterior target, IdxCell addedCell)
        {
            var cells = _cells.ToList();
            cells.Insert(InsertPosition(addedCell), addedCell);
            int leftCount = 0;
            int leftSize = 0;
            int boundarySize = (_size + addedCell.CellSize) / 2;
            for (int i = 0; i < cells.Count; i++)
            {
                var cell = cells[i];
                if (leftSize + cell.CellSize < boundarySize)
                {
                    leftSize += cell.CellSize;
                    leftCount++;
                }
                else
                    break;
            }
            _cells = cells.Take(leftCount).ToList();
            var resultCell = cells.Skip(leftCount).First();
            target._cells = cells.Skip(1 + leftCount).ToList();
            target._leftMost = resultCell.ChildPage;

            _cellsCount = _cells.Count;
            _dirty = true;
            _size = 16 + _cells.Sum(c => c.CellSize);
            for (int i = 0; i < _cellsCount; i++)
                _cells[i].Ordinal = i;

            target._cellsCount = target._cells.Count;
            target._dirty = true;
            target._size = 16 + target._cells.Sum(c => c.CellSize);
            for (int i = 0; i < target._cellsCount; i++)
                target._cells[i].Ordinal = i;

            return resultCell.Key;
        }

        public IdxKey Merge(IdxInterior node, IdxCell parent)
        {
            var cells = new List<IdxCell>(_cellsCount + 1 + node._cellsCount);
            cells.AddRange(_cells);
            cells.Add(IdxCell.CreateInteriorCell(parent.Key, node.LeftmostPage));
            cells.AddRange(node._cells);
            _cells = cells;
            _cellsCount = _cells.Count;
            FixOrdinals();
            _size = 16 + _cells.Sum(c => c.CellSize);
            return null;
        }
    }
}
