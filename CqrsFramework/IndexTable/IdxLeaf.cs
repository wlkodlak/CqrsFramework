using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public class IdxLeaf
    {
        public IdxLeaf(byte[] data)
        {
        }

        public byte[] Save()
        {
            return null;
        }

        public int Next { get { return 0; } set { } }
        public int CellsCount { get { return 0; } }
        public bool IsSmall { get { return true; } }
        public bool IsFull { get { return false; } }
    }
}
