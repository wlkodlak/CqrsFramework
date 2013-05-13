using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public class IdxNode
    {
        private int _pageNumber = 0;
        private List<IdxCell> _cells = new List<IdxCell>();
        private int _leafSize = 16;
        private bool _dirty = false;
        private int _next = 0;

        public IdxNode(byte[] data)
        {
            if (data == null)
                return;
            using (var reader = new BinaryReader(new MemoryStream(data)))
            {
                var pageType = reader.ReadByte();
                int cellsCount = reader.ReadByte();
                reader.ReadInt16();
                _next = reader.ReadInt32();
                reader.ReadBytes(8);
                for (int i = 0; i < cellsCount; i++)
                {
                    var cell = IdxCell.LoadLeafCell(reader);
                    cell.Ordinal = i;
                    _cells.Add(cell);
                    _leafSize += cell.CellSize;
                }
            }
        }

        public IdxCell FindByKey(IdxKey key)
        {
            foreach (var cell in _cells)
            {
                if (key <= cell.Key)
                    return cell;
            }
            return null;
        }

        public void RemoveCell(int index)
        {
            var originalCell = _cells[index];
            _cells.RemoveAt(index);
            foreach (var cell in _cells)
            {
                if (cell.Ordinal > index)
                    cell.Ordinal--;
            }
            _leafSize -= originalCell.CellSize;
            _dirty = true;
        }

        public byte[] Save()
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(new byte[4] { 1, (byte)_cells.Count, 0, 0 });
                writer.Write(_next);
                writer.Write(new byte[8]);
                foreach (var cell in _cells)
                    cell.SaveLeafCell(writer);
            }
            _dirty = false;
            return buffer;
        }

        public int Next
        {
            get { return _next; }
            set
            {
                _next = value;
                _dirty = true;
            }
        }
        public int CellsCount { get { return _cells.Count; } }
        public bool IsSmall { get { return _leafSize < PagedFile.PageSize / 4; } }
        public bool IsFull { get { return _leafSize >= PagedFile.PageSize - 128; } }
        public bool IsDirty { get { return _dirty; } }

        public void AddCell(IdxCell cell)
        {
            int position = PositionForInsert(cell.Key);
            _cells.Insert(position, cell);
            _leafSize += cell.CellSize;
            for (int i = 0; i < _cells.Count; i++)
                _cells[i].Ordinal = i;
            _dirty = true;
        }

        private int PositionForInsert(IdxKey key)
        {
            for (int i = 0; i < _cells.Count; i++)
            {
                var cellKey = _cells[i].Key;
                if (key <= cellKey)
                    return i;
            }
            return _cells.Count;
        }

        public IdxCell GetCell(int number)
        {
            return _cells[number];
        }

        public int PageNumber
        {
            get { return _pageNumber; }
            set { _pageNumber = value; }
        }

        public IdxKey SplitLeaf(IdxNode newPage, IdxCell addedCell)
        {
            int position = PositionForInsert(addedCell.Key);
            var newCells = _cells.ToList();
            newCells.Insert(position, addedCell);
            int leftCount = 0;
            int leftLength = 16;
            foreach (var cell in newCells)
            {
                if (leftLength < PagedFile.PageSize / 2)
                {
                    leftLength += cell.CellSize;
                    leftCount++;
                }
            }

            _dirty = true;
            _cells = newCells.Take(leftCount).ToList();
            _leafSize = 16 + _cells.Sum(c => c.CellSize);
            
            newPage._dirty = true;
            newPage._cells = newCells.Skip(leftCount).ToList();
            newPage._leafSize = 16 + newPage._cells.Sum(c => c.CellSize);

            newPage._next = _next;
            _next = newPage._pageNumber;
            return newPage._cells[0].Key;
        }
    }
}
