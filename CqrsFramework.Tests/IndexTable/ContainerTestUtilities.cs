using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    public class MemoryPagedFile : IPagedFile
    {
        public bool Disposed = false;
        public HashSet<int> ReadPages = new HashSet<int>();
        public HashSet<int> WrittenPages = new HashSet<int>();
        public List<byte[]> Pages = new List<byte[]>();
        public bool IncreasedSize = false;
        public bool DecreasedSize = false;
        public bool ChangedSize { get { return IncreasedSize || DecreasedSize; } }

        public MemoryPagedFile()
        {
        }

        public MemoryPagedFile(int size)
        {
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
                    Pages[i] = new byte[PagedFile.PageSize];
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
                    Pages.Add(new byte[PagedFile.PageSize]);
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
                Pages[page] = bytes = new byte[PagedFile.PageSize];
            return bytes;
        }

        public void SetPage(int page, byte[] data)
        {
            WrittenPages.Add(page);
            if (Pages[page] == null)
                Pages[page] = new byte[PagedFile.PageSize];
            Array.Copy(data, Pages[page], PagedFile.PageSize);
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
            builder._leaf = new IdxLeaf(null);
            builder._leaf.NextLeaf = next;
            return builder;
        }

        public static NodeBuilder Interior(int leftMost)
        {
            var builder = new NodeBuilder(false);
            builder._interior = new IdxInterior(null);
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
            var header = new IdxHeader(null);
            header.FreePagesList = freeList;
            header.TotalPagesCount = totalPages;
            for (int i = 0; i < roots.Length; i++)
                header.SetTreeRoot(i, roots[i]);
            return header.Save();
        }

        public static byte[] CreateFreeList(int next, params int[] pages)
        {
            var freelist = new IdxFreeList(null);
            freelist.Next = next;
            foreach (var page in pages)
                freelist.Add(page);
            return freelist.Save();
        }
    }
}
