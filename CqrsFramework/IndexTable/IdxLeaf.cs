using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public class IdxLeaf : IdxPageBase, IIdxNode
    {
        private int _pageSize;
        private int _cellsCount = 0;
        private List<IdxCell> _cells = new List<IdxCell>();
        private int _next = 0;
        private int _size = 16;
        private int _smallSize;
        private int _fullSize;

        public IdxLeaf(byte[] bytesToLoad, int pageSize)
        {
            _pageSize = pageSize;
            _smallSize = pageSize / 4;
            _fullSize = pageSize - 128;
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
                {
                    var cell = IdxCell.LoadLeafCell(reader, _pageSize);
                    cell.Ordinal = i;
                    _cells.Add(cell);
                    _size += cell.CellSize;
                    cell.ValueChanged += CellValueChanged;
                }
            }
        }

        private void CellValueChanged(object sender, EventArgs e)
        {
            SetDirty(true);
        }

        public bool IsLeaf { get { return true; } }
        public int CellsCount { get { return _cellsCount; } }
        public bool IsSmall { get { return _size < _smallSize; } }
        public bool IsFull { get { return _size > _fullSize; } }

        public int NextLeaf
        {
            get { return _next; }
            set
            {
                SetDirty(true);
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
            SetDirty(true);
            for (int i = 0; i < _cellsCount; i++)
                _cells[i].Ordinal = i;
        }

        public IdxCell GetCell(int index)
        {
            return _cells[index];
        }

        public override byte[] Save()
        {
            SetDirty(false);
            var buffer = new byte[_pageSize];
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
            SetDirty(true);
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
            _cells.Insert(positionToAdd, cell);
            _cellsCount++;

            rightNode._next = _next;
            _next = rightNode.PageNumber;

            int cellsToMove = CellsToMove(this, rightNode, true);
            return MergeToRight(rightNode, cellsToMove);
        }

        public IdxKey Merge(IdxLeaf rightLeaf)
        {
            if (_size < rightLeaf._size)
            {
                int cellsToMove = CellsToMove(rightLeaf, this, false);
                if (cellsToMove == 0)
                    return MergeToSingle(rightLeaf);
                else
                    return MergeToLeft(rightLeaf, cellsToMove);
            }
            else
            {
                int cellsToMove = CellsToMove(this, rightLeaf, true);
                if (cellsToMove == 0)
                    return MergeToSingle(rightLeaf);
                else
                    return MergeToRight(rightLeaf, cellsToMove);
            }
        }

        private IdxKey MergeToSingle(IdxLeaf rightLeaf)
        {
            _next = rightLeaf._next;
            _cells.AddRange(rightLeaf._cells);
            SetDirty(true);
            CompleteFix();
            return null;
        }

        private int CellsToMove(IdxLeaf fromLeaf, IdxLeaf toLeaf, bool takeFromEnd)
        {
            var remainingSize = fromLeaf._size;
            var idealSize = Math.Max((toLeaf._size + fromLeaf._size) / 2, _smallSize);
            var missingSize = _smallSize - toLeaf._size;
            var cellsToMove = 0;
            var fromPosition = takeFromEnd ? fromLeaf._cellsCount - 1 : 0;
            while (missingSize > 0)
            {
                var cell = fromLeaf._cells[fromPosition];
                missingSize -= cell.CellSize;
                remainingSize -= cell.CellSize;
                if (remainingSize < _smallSize)
                    return 0;
                fromPosition += takeFromEnd ? -1 : 1;
                cellsToMove++;
            }
            while (remainingSize > idealSize)
            {
                var cell = fromLeaf._cells[fromPosition];
                missingSize -= cell.CellSize;
                remainingSize -= cell.CellSize;
                if (remainingSize >= _smallSize)
                    cellsToMove++;
                fromPosition += takeFromEnd ? -1 : 1;
            }
            return cellsToMove;
        }

        private IdxKey MergeToLeft(IdxLeaf rightLeaf, int cellsToMove)
        {
            _cells.AddRange(rightLeaf._cells.Take(cellsToMove));
            rightLeaf._cells.RemoveRange(0, cellsToMove);
            SetDirty(true);
            rightLeaf.SetDirty(true);
            CompleteFix();
            rightLeaf.CompleteFix();
            return rightLeaf._cells[0].Key;
        }

        private IdxKey MergeToRight(IdxLeaf rightLeaf, int cellsToMove)
        {
            int leftStartIndex = _cellsCount - cellsToMove;
            var rightCells = new List<IdxCell>(cellsToMove + rightLeaf._cellsCount);
            rightCells.AddRange(_cells.Skip(leftStartIndex).Take(cellsToMove));
            rightCells.AddRange(rightLeaf._cells);
            rightLeaf._cells = rightCells;
            _cells.RemoveRange(leftStartIndex, cellsToMove);
            SetDirty(true);
            rightLeaf.SetDirty(true);
            CompleteFix();
            rightLeaf.CompleteFix();
            return rightLeaf._cells[0].Key;
        }

        private void CompleteFix()
        {
            _cellsCount = _cells.Count;
            _size = 16;
            for (int i = 0; i < _cells.Count; i++)
            {
                _cells[i].Ordinal = i;
                _size += _cells[i].CellSize;
            }
        }
    }
}
