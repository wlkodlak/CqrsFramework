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

        private const int SmallSize = PagedFile.PageSize / 4;
        private const int FullSize = PagedFile.PageSize - 128;

        public int PageNumber { get; set; }
        public int CellsCount { get { return _cellsCount; } }
        public bool IsSmall { get { return _size < SmallSize; } }
        public bool IsFull { get { return _size > FullSize; } }
        public bool IsDirty { get { return _dirty; } }

        public void AddCell(IdxCell cell)
        {
            int position = InsertPosition(cell);
            _cells.Insert(position, cell);
            _dirty = true;
            CompleteFix();
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

        private void CompleteFix()
        {
            _cellsCount = _cells.Count;
            _size = 16;
            for (int i = 0; i < _cellsCount; i++)
            {
                _cells[i].Ordinal = i;
                _size += _cells[i].CellSize;
            }
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
            _dirty = true;
            _cells.RemoveAt(index);
            CompleteFix();
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
            _dirty = true;
            target._dirty = true;

            _cells.Insert(InsertPosition(addedCell), addedCell);
            var parentCell = IdxCell.CreateInteriorCell(_cells[_cellsCount].Key, target.PageNumber);
            target._leftMost = _cells[_cellsCount].ChildPage;
            _cells.RemoveAt(_cellsCount);

            int moveCount = CellsToMove(this, target, parentCell, true);
            return MoveToRight(target, parentCell, moveCount);
        }

        public IdxKey Merge(IdxInterior node, IdxCell parent)
        {
            if (_size < node._size)
            {
                int cellsToMove = CellsToMove(node, this, parent, false);
                if (cellsToMove == 0)
                    return MergeToSingle(node, parent);
                else
                    return MoveToLeft(node, parent, cellsToMove);
            }
            else
            {
                int cellsToMove = CellsToMove(this, node, parent, true);
                if (cellsToMove == 0)
                    return MergeToSingle(node, parent);
                else
                    return MoveToRight(node, parent, cellsToMove);
            }
        }

        private int CellsToMove(IdxInterior fromNode, IdxInterior toNode, IdxCell parent, bool pickLast)
        {
            var missingSize = SmallSize - toNode._size;
            var idealSize = Math.Max((fromNode._size + toNode._size) / 2, SmallSize);
            var remainingSize = fromNode._size;
            var count = 0;
            var parentSize = parent.CellSize;
            var fromPosition = pickLast ? fromNode._cellsCount - 1 : 0;
            var plusPosition = pickLast ? -1 : 1;
            while (missingSize > 0)
            {
                missingSize -= parentSize;
                parentSize = fromNode._cells[fromPosition].CellSize;
                remainingSize -= parentSize;
                if (remainingSize < SmallSize)
                    return 0;
                fromPosition += plusPosition;
                count++;
            }
            while (remainingSize > idealSize)
            {
                missingSize -= parentSize;
                parentSize = fromNode._cells[fromPosition].CellSize;
                remainingSize -= parentSize;
                if (remainingSize >= idealSize)
                {
                    fromPosition += plusPosition;
                    count++;
                }
            }
            return count;
        }

        private IdxKey MergeToSingle(IdxInterior node, IdxCell parent)
        {
            var cells = new List<IdxCell>(_cellsCount + 1 + node._cellsCount);
            cells.AddRange(_cells);
            cells.Add(IdxCell.CreateInteriorCell(parent.Key, node.LeftmostPage));
            cells.AddRange(node._cells);
            _cells = cells;
            CompleteFix();
            return null;
        }

        private IdxKey MoveToLeft(IdxInterior rightNode, IdxCell parent, int count)
        {
            var movedCells = new List<IdxCell>(count + 1);
            movedCells.Add(IdxCell.CreateInteriorCell(parent.Key, rightNode.LeftmostPage));
            movedCells.AddRange(rightNode._cells.Take(count));
            _cells.AddRange(movedCells.Take(count));
            rightNode._cells.RemoveRange(0, count);
            rightNode.LeftmostPage = movedCells[count].ChildPage;
            CompleteFix();
            rightNode.CompleteFix();
            return movedCells[count].Key;
        }

        private IdxKey MoveToRight(IdxInterior rightNode, IdxCell parent, int count)
        {
            var startIndex = _cellsCount - count;
            var movedCells = new List<IdxCell>(count + 1);
            movedCells.AddRange(_cells.Skip(startIndex));
            movedCells.Add(IdxCell.CreateInteriorCell(parent.Key, rightNode.LeftmostPage));
            movedCells.AddRange(rightNode._cells);
            _cells.RemoveRange(startIndex, count);
            rightNode._cells = movedCells.Skip(1).ToList();
            rightNode.LeftmostPage = movedCells[0].ChildPage;
            CompleteFix();
            rightNode.CompleteFix();
            return movedCells[0].Key;
        }
    }
}
