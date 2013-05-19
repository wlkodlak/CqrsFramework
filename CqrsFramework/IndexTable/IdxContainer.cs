using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public interface IIdxContainer : IDisposable
    {
        IIdxNode ReadTree(int tree);
        IIdxNode WriteTree(int tree);
        void UnlockRead(int tree);
        void CommitWrite(int tree);
        void RollbackWrite(int tree);
        IdxOverflow GetOverflow(int tree, int page);
        IdxOverflow CreateOverflow(int tree);
        IIdxNode GetNode(int tree, int page);
        IdxLeaf CreateLeaf(int tree);
        IdxInterior CreateInterior(int tree);
        void Delete(int tree, int page);
        void SetTreeRoot(int tree, IIdxNode root);
    }

    public class IdxContainer : IIdxContainer
    {
        private IPagedFile _file;
        private IdxHeader _header;
        private List<IdxFreeList> _freelists;
        private int[] _growPages;
        private TransactionInfo[] _transactions;

        private class TransactionInfo
        {
            public List<int> AllocatedPages = new List<int>(128);
            public List<int> FreedPages = new List<int>(32);
            public bool RootChanged;
            public int TreeRoot;
            public Dictionary<int, IIdxPage> UsedPages = new Dictionary<int, IIdxPage>(1024);
        }

        public IdxContainer(IPagedFile file)
        {
            _file = file;
            _freelists = new List<IdxFreeList>();
            if (_file.GetSize() > 0)
            {
                _header = new IdxHeader(_file.GetPage(0));
                var firstFreeList = new IdxFreeList(_file.GetPage(_header.FreePagesList));
                firstFreeList.PageNumber = _header.FreePagesList;
                _freelists.Add(firstFreeList);
            }
            _growPages = new int[] { 16, 4, 64, 8, 256, 16, 1024, 64, 8 * 1024, 256, 32 * 1024, 1024, 256 * 1024, 4096 };
            _transactions = new TransactionInfo[16];
        }

        private void InitializeFile()
        {
            _file.SetSize(4);
            _header = new IdxHeader(null);
            _header.FreePagesList = 1;
            _header.TotalPagesCount = 4;
            var freeList = new IdxFreeList(null);
            freeList.PageNumber = 1;
            freeList.Add(4);
            freeList.Add(3);
            _freelists.Add(freeList);
            _file.SetPage(0, _header.Save());
            _file.SetPage(1, freeList.Save());
        }

        private void CommitHeader()
        {
            if (_header.IsDirty)
                _file.SetPage(0, _header.Save());
            foreach (var freelist in _freelists)
                if (freelist.IsDirty)
                    _file.SetPage(freelist.PageNumber, freelist.Save());
        }

        private int AllocPage()
        {
            IdxFreeList freeList = _freelists[0];
            if (!freeList.IsEmpty)
                return freeList.Alloc();
            else if (freeList.Next != 0)
            {
                int page = freeList.PageNumber;
                _header.FreePagesList = freeList.Next;
                _freelists.RemoveAt(0);
                if (_freelists.Count == 0)
                {
                    freeList = new IdxFreeList(_file.GetPage(_header.FreePagesList));
                    freeList.PageNumber = _header.FreePagesList;
                    _freelists.Add(freeList);
                }
                return page;
            }
            else
            {
                var freePages = FreePagesAfterGrow();
                ReplaceFreeLists(freePages);
                FillFreeLists(freePages);
                return _freelists[0].Alloc();
            }
        }

        private int GetGrowPagesCount()
        {
            int count = _header.TotalPagesCount;
            for (int i = 0; i < _growPages.Length; i += 2)
                if (count < _growPages[i])
                    return _growPages[i + 1];
            return 32 * 1024;
        }

        private List<int> FreePagesAfterGrow()
        {
            int newPages = GetGrowPagesCount();
            int originalSize = _header.TotalPagesCount;
            _header.TotalPagesCount += newPages;
            _file.SetSize(_header.TotalPagesCount);

            var freePages = new List<int>(newPages + _freelists.Count);
            for (int i = _header.TotalPagesCount - 1; i >= originalSize; i--)
                freePages.Add(i);
            foreach (var page in _freelists.AsEnumerable().Reverse())
                freePages.Add(page.PageNumber);
            return freePages;
        }

        private void ReplaceFreeLists(List<int> freePages)
        {
            int newFreeListsCount = (freePages.Count + IdxFreeList.Capacity - 1) / IdxFreeList.Capacity;
            _freelists.Clear();
            for (int i = 0; i < newFreeListsCount; i++)
            {
                var newFreeList = new IdxFreeList(null);
                int index = freePages.Count - 1;
                newFreeList.PageNumber = freePages[index];
                freePages.RemoveAt(index);
                _freelists.Add(newFreeList);
            }
            for (int i = 1; i < newFreeListsCount; i++)
                _freelists[i - 1].Next = _freelists[i].PageNumber;
        }

        private void FillFreeLists(List<int> freePages)
        {
            int pagesInList = freePages.Count % IdxFreeList.Capacity;
            if (pagesInList == 0)
                pagesInList = IdxFreeList.Capacity;
            int listIndex = 0;
            for (int i = 0; i < freePages.Count; i++)
            {
                if (pagesInList > 0)
                {
                    pagesInList--;
                    _freelists[listIndex].Add(freePages[i]);
                }
                else
                {
                    listIndex++;
                    pagesInList = IdxFreeList.Capacity - 1;
                    _freelists[listIndex].Add(freePages[i]);
                }
            }
        }

        public IIdxNode ReadTree(int tree)
        {
            if (_header == null)
                return null;
            int page = _header.GetTreeRoot(tree);
            if (page == 0)
                return null;
            return ReadNode(page);
        }

        private IIdxNode ReadNode(int page)
        {
            var bytes = _file.GetPage(page);
            IIdxNode node;
            if (DetectLeaf(bytes))
                node = new IdxLeaf(bytes);
            else
                node = new IdxInterior(bytes);
            node.PageNumber = page;
            return node;
        }

        private bool DetectLeaf(byte[] bytes)
        {
            return bytes[0] == 1;
        }

        public IIdxNode WriteTree(int tree)
        {
            if (_header == null)
                InitializeFile();
            _transactions[tree] = new TransactionInfo();
            _transactions[tree].TreeRoot = _header.GetTreeRoot(tree);
            return null;
        }

        public void UnlockRead(int tree)
        {
        }

        public void CommitWrite(int tree)
        {
            var tran = _transactions[tree];
            if (tran.RootChanged)
                _header.SetTreeRoot(tree, tran.TreeRoot);
            CommitHeader();
            foreach (var page in tran.UsedPages.Values)
            {
                if (!page.IsDirty)
                    continue;
                _file.SetPage(page.PageNumber, page.Save());
            }
        }

        public void RollbackWrite(int tree)
        {
        }

        public IdxOverflow GetOverflow(int tree, int page)
        {
            var overflow = new IdxOverflow(_file.GetPage(page));
            overflow.PageNumber = page;
            return overflow;
        }

        public IdxOverflow CreateOverflow(int tree)
        {
            var overflow = new IdxOverflow(null);
            overflow.PageNumber = AllocPage();
            _transactions[tree].UsedPages[overflow.PageNumber] = overflow;
            return overflow;
        }

        public IIdxNode GetNode(int tree, int page)
        {
            return ReadNode(page);
        }

        public IdxLeaf CreateLeaf(int tree)
        {
            int page = AllocPage();
            var leaf = new IdxLeaf(null);
            leaf.PageNumber = page;
            _transactions[tree].UsedPages[leaf.PageNumber] = leaf;
            return leaf;
        }

        public IdxInterior CreateInterior(int tree)
        {
            throw new NotImplementedException();
        }

        public void Delete(int tree, int page)
        {
        }

        public void SetTreeRoot(int tree, IIdxNode root)
        {
            _transactions[tree].TreeRoot = root.PageNumber;
            _transactions[tree].RootChanged = true;
        }

        public void Dispose()
        {
            _file.Dispose();
        }
    }
}
