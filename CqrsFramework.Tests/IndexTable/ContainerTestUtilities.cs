using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    public class MemoryPagedFile : IIdxPagedFile
    {
        public bool Disposed = false;
        public HashSet<int> ReadPages = new HashSet<int>();
        public HashSet<int> WrittenPages = new HashSet<int>();
        public List<byte[]> Pages = new List<byte[]>();
        public bool IncreasedSize = false;
        public bool DecreasedSize = false;
        public bool ChangedSize { get { return IncreasedSize || DecreasedSize; } }
        public int PageSize { get { return _pageSize; } }

        private int _pageSize;

        public MemoryPagedFile(bool smallSize = false)
        {
            _pageSize = smallSize ? 512 : 4096;
        }

        public MemoryPagedFile(int size, bool smallPages = false)
        {
            _pageSize = smallPages ? 512 : 4096;
            for (int i = Pages.Count; i < size; i++)
                Pages.Add(null);
        }

        public void ClearStats()
        {
            IncreasedSize = false;
            DecreasedSize = false;
            ReadPages.Clear();
            WrittenPages.Clear();
            Disposed = false;
            for (int i = 0; i < Pages.Count; i++)
                if (Pages[i] == null)
                    Pages[i] = new byte[4096];
        }

        public int GetSize()
        {
            return Pages.Count;
        }

        public void SetSize(int finalCount)
        {
            if (finalCount == Pages.Count)
                return;
            else if (finalCount > Pages.Count)
            {
                IncreasedSize = true;
                for (int i = Pages.Count; i < finalCount; i++)
                    Pages.Add(new byte[4096]);
            }
            else
            {
                DecreasedSize = true;
                Pages.RemoveRange(finalCount, Pages.Count - finalCount);
            }
        }

        public byte[] GetPage(int page)
        {
            ReadPages.Add(page);
            var bytes = Pages[page];
            if (bytes == null)
                Pages[page] = bytes = new byte[4096];
            return bytes;
        }

        public void SetPage(int page, byte[] data)
        {
            WrittenPages.Add(page);
            if (Pages[page] == null)
                Pages[page] = new byte[4096];
            Array.Copy(data, Pages[page], 4096);
        }

        public void Dispose()
        {
            Disposed = true;
        }
    }

    public class NodeBuilder
    {
        private bool _isLeaf;
        private IdxLeaf _leaf;
        private IdxInterior _interior;

        private NodeBuilder(bool isLeaf)
        {
            _isLeaf = isLeaf;
        }

        public static NodeBuilder Leaf(int next)
        {
            var builder = new NodeBuilder(true);
            builder._leaf = new IdxLeaf(null, 4096);
            builder._leaf.NextLeaf = next;
            return builder;
        }

        public static NodeBuilder Interior(int leftMost)
        {
            var builder = new NodeBuilder(false);
            builder._interior = new IdxInterior(null, 4096);
            builder._interior.LeftmostPage = leftMost;
            return builder;
        }

        public NodeBuilder AddCell(IdxCell cell)
        {
            if (_isLeaf)
                _leaf.AddCell(cell);
            else
                _interior.AddCell(cell);
            return this;
        }

        public byte[] ToBytes()
        {
            if (_isLeaf)
                return _leaf.Save();
            else
                return _interior.Save();
        }
    }

    public class ContainerTestUtilities
    {
        public static byte[] CreateHeader(int freeList, int totalPages, params int[] roots)
        {
            var header = new IdxHeader(null, 4096);
            header.FreePagesList = freeList;
            header.TotalPagesCount = totalPages;
            for (int i = 0; i < roots.Length; i++)
                header.SetTreeRoot(i, roots[i]);
            return header.Save();
        }

        public static byte[] CreateFreeList(int next, params int[] pages)
        {
            var freelist = new IdxFreeList(null, 4096);
            freelist.Next = next;
            foreach (var page in pages)
                freelist.Add(page);
            return freelist.Save();
        }

        public static byte[] CreateBytes(Random random, int length)
        {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            return bytes;
        }

        public static byte[] CreateOverflow(int next, int offset, byte[] bytes)
        {
            var overflow = new IdxOverflow(null, 4096);
            overflow.Next = next;
            overflow.WriteData(bytes, offset);
            return overflow.Save();
        }
    }
}
