using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public interface IIdxTree
    {
        void Insert(IdxKey key, byte[] value);
        void Update(IdxKey key, byte[] value);
        void Delete(IdxKey key);
        IEnumerable<KeyValuePair<IdxKey, byte[]>> Select(IdxKey min, IdxKey max);
    }

    public class IdxTree : IIdxTree
    {
        private IIdxContainer _container;
        private int _tree;

        public IdxTree(IIdxContainer container, int tree)
        {
            _container = container;
            _tree = tree;
        }

        public void Insert(IdxKey key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Update(IdxKey key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Delete(IdxKey key)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<KeyValuePair<IdxKey, byte[]>> Select(IdxKey min, IdxKey max)
        {
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            var node = _container.ReadTree(_tree);
            if (node == null)
                return result;
            try
            {
                while (!node.IsLeaf)
                {
                    var interior = (IdxInterior)node;
                    int childPage = interior.LeftmostPage;
                    for (int i = 0; i < interior.CellsCount; i++)
                    {
                        var cell = interior.GetCell(i);
                        if (min >= cell.Key)
                            childPage = cell.ChildPage;
                    }
                    if (childPage == 0)
                        throw new NullReferenceException(string.Format("Null child page in node {0}", node.PageNumber));
                    node = _container.GetNode(_tree, childPage);
                }

                var leaf = (IdxLeaf)node;
                var cellIndex = -1;
                for (int i = 0; i < leaf.CellsCount; i++)
                {
                    var cell = leaf.GetCell(i);
                    if (min <= cell.Key)
                    {
                        cellIndex = i;
                        break;
                    }
                }
                if (cellIndex == -1)
                    return result;

                while (leaf != null)
                {
                    if (cellIndex >= leaf.CellsCount)
                    {
                        cellIndex = 0;
                        leaf = (IdxLeaf)_container.GetNode(_tree, leaf.NextLeaf);
                    }
                    else
                    {
                        var cell = leaf.GetCell(cellIndex);
                        if (cell.Key <= max)
                        {
                            result.Add(new KeyValuePair<IdxKey, byte[]>(cell.Key, ReadCellValue(cell)));
                            cellIndex++;
                        }
                        else
                            leaf = null;
                    }
                }

                return result;
            }
            finally
            {
                _container.UnlockRead(_tree);
            }
        }

        private byte[] ReadCellValue(IdxCell cell)
        {
            if (cell.OverflowPage == 0)
                return cell.ValueBytes;
            var buffer = new byte[4096];
            Array.Copy(cell.ValueBytes, buffer, cell.ValueLength);
            var offset = cell.ValueLength;
            var overflowPage = cell.OverflowPage;
            while (overflowPage != 0)
            {
                var overflow = _container.GetOverflow(_tree, overflowPage);
                var neededCapacity = offset + overflow.LengthInPage;
                if (neededCapacity > buffer.Length)
                {
                    int newSize = buffer.Length * 2;
                    while (neededCapacity > newSize)
                        newSize *= 2;
                    Array.Resize(ref buffer, newSize);
                }
                var read = overflow.ReadData(buffer, offset);
                offset += read;
                overflowPage = overflow.Next;
            }
            Array.Resize(ref buffer, offset);
            return buffer;
        }
    }
}
