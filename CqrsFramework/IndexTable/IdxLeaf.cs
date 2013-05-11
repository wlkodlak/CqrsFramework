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
        private List<IdxCell> _cells = new List<IdxCell>();
        private int _leafSize = 16;

        public IdxLeaf(byte[] data)
        {
            if (data == null)
                return;
            using (var reader = new BinaryReader(new MemoryStream(data)))
            {
                var pageType = reader.ReadByte();
                int cellsCount = reader.ReadByte();
                reader.ReadInt16();
                var nextPage = reader.ReadInt32();
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

        public byte[] Save()
        {
            return null;
        }

        public int Next { get { return 0; } set { } }
        public int CellsCount { get { return _cells.Count; } }
        public bool IsSmall { get { return _leafSize < PagedFile.PageSize / 4; } }
        public bool IsFull { get { return _leafSize >= PagedFile.PageSize - 128; } }

        public void AddCell(IdxCell cell)
        {
            _cells.Add(cell);
            _leafSize += cell.CellSize;
            cell.Ordinal = _cells.Count - 1;
        }

        public IdxCell GetCell(int number)
        {
            return _cells[number];
        }
    }
}
