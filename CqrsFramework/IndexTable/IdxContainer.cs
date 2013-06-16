using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

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

        int GetPageSize();
    }

    public class IdxContainer : IIdxContainer
    {
        private object _lock = new object();
        private bool _disposed = false;
        private IIdxPagedFile _file;
        private IdxHeader _header;
        private List<IdxFreeList> _freelists;
        private int[] _growPages;
        private TransactionInfo[] _transactions;
        private Dictionary<int, CachedPage> _cache;
        private Timer _timer;
        private int _pageSize;

        private class TransactionInfo
        {
            public int ReadersCount = 0;
            public bool HasWriter;
            public List<int> AllocatedPages = new List<int>(128);
            public List<int> FreedPages = new List<int>(32);
            public bool RootChanged;
            public int TreeRoot;
            public Dictionary<int, IIdxPage> UsedPages = new Dictionary<int, IIdxPage>(1024);
            public Thread WriterThread;
            public List<Thread> ReaderThreads = new List<Thread>();
        }

        private class CachedPage
        {
            public int Number;
            public int Timeout;
            public IIdxPage Contents;

            public CachedPage(IIdxPage contents)
            {
                this.Contents = contents;
                this.Timeout = 2;
                this.Number = contents.PageNumber;
            }

            public T UseAs<T>()
            {
                MarkUsed();
                return (T)Contents;
            }

            public bool ShouldEject()
            {
                return Timeout == 0;
            }

            public void MarkUsed()
            {
                Timeout = Math.Min(4, Timeout + 2);
            }

            public void EjectionCycle()
            {
                Timeout--;
            }
        }

        public static IdxContainer OpenStream(System.IO.Stream stream)
        {
            return new IdxContainer(new IdxPagedFile(stream, 4096));
        }

        public IdxContainer(IIdxPagedFile file)
        {
            _file = file;
            _pageSize = file.PageSize;
            _freelists = new List<IdxFreeList>();
            if (_file.GetSize() > 0)
            {
                _header = new IdxHeader(_file.GetPage(0), _pageSize);
                var firstFreeList = new IdxFreeList(_file.GetPage(_header.FreePagesList), _pageSize);
                firstFreeList.PageNumber = _header.FreePagesList;
                _freelists.Add(firstFreeList);
            }
            _growPages = new int[] { 16, 4, 64, 8, 256, 16, 1024, 64, 8 * 1024, 256, 32 * 1024, 1024, 256 * 1024, 4096 };
            _transactions = new TransactionInfo[16];
            for (int i = 0; i < 16; i++)
                _transactions[i] = new TransactionInfo() { TreeRoot = i };
            _cache = new Dictionary<int, CachedPage>();
            _timer = new Timer(CacheCleanupTimerCallback, null, 15000, 15000);
        }

        private void CacheCleanupTimerCallback(object state)
        {
            lock (_lock)
            {
                if (_disposed || _cache.Count == 0)
                    return;
                foreach (var item in _cache.Values)
                    item.EjectionCycle();
                var pagesForRemoval = _cache.Where(i => i.Value.ShouldEject()).Select(i => i.Key).ToList();
                foreach (var page in pagesForRemoval)
                    _cache.Remove(page);
            }
        }

        private void InitializeFile()
        {
            _file.SetSize(4);
            _header = new IdxHeader(null, _pageSize);
            _header.FreePagesList = 1;
            _header.TotalPagesCount = 4;
            var freeList = new IdxFreeList(null, _pageSize);
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
                    freeList = new IdxFreeList(_file.GetPage(_header.FreePagesList), _pageSize);
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

        private void FreePage(int page)
        {
            if (!_freelists[0].IsFull)
                _freelists[0].Add(page);
            else
            {
                var freelist = new IdxFreeList(null, _pageSize);
                freelist.PageNumber = page;
                freelist.Next = _freelists[0].PageNumber;
                _freelists.Insert(0, freelist);
                _header.FreePagesList = page;
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
            var freelistCapacity = IdxFreeList.Capacity(_pageSize);
            int newFreeListsCount = (freePages.Count + freelistCapacity - 1) / freelistCapacity;
            _freelists.Clear();
            for (int i = 0; i < newFreeListsCount; i++)
            {
                var newFreeList = new IdxFreeList(null, _pageSize);
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
            var freelistCapacity = IdxFreeList.Capacity(_pageSize);
            int pagesInList = freePages.Count % freelistCapacity;
            if (pagesInList == 0)
                pagesInList = freelistCapacity;
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
                    pagesInList = freelistCapacity - 1;
                    _freelists[listIndex].Add(freePages[i]);
                }
            }
        }

        public int GetPageSize()
        {
            return _pageSize;
        }

        public IIdxNode ReadTree(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                if (_header == null)
                    return null;
                var tran = _transactions[tree];
                if (tran.HasWriter)
                {
                    if (tran.WriterThread == Thread.CurrentThread)
                        throw new IdxLockException();
                    while (tran.HasWriter)
                    {
                        ThrowIfDisposed();
                        Monitor.Wait(_lock);
                    }
                }
                tran.ReadersCount++;
                tran.ReaderThreads.Add(Thread.CurrentThread);
                int page = _header.GetTreeRoot(tree);
                if (page == 0)
                    return null;
                return ReadNode(page);
            }
        }

        private IIdxNode ReadNode(int page)
        {
            CachedPage cached;
            if (_cache.TryGetValue(page, out cached))
                return cached.UseAs<IIdxNode>();

            var bytes = _file.GetPage(page);
            IIdxNode node;
            if (DetectLeaf(bytes))
                node = new IdxLeaf(bytes, _pageSize);
            else
                node = new IdxInterior(bytes, _pageSize);
            node.PageNumber = page;

            _cache[page] = new CachedPage(node);
            return node;
        }

        private bool DetectLeaf(byte[] bytes)
        {
            return bytes[0] == 1;
        }

        public IIdxNode WriteTree(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                if (_header == null)
                    InitializeFile();
                var tran = _transactions[tree];
                var currentThread = Thread.CurrentThread;
                if (tran.HasWriter || tran.ReadersCount > 0)
                {
                    if (tran.WriterThread == currentThread)
                        throw new IdxLockException();
                    foreach (var reader in tran.ReaderThreads)
                        if (reader == currentThread)
                            throw new IdxLockException();
                    while (tran.HasWriter || tran.ReadersCount > 0)
                    {
                        ThrowIfDisposed();
                        Monitor.Wait(_lock);
                    }
                }
                tran.HasWriter = true;
                tran.WriterThread = currentThread;
                var rootPage = _header.GetTreeRoot(tree);
                if (rootPage == 0)
                    return null;
                return GetNode(tree, rootPage);
            }
        }

        public void UnlockRead(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                tran.ReadersCount--;
                tran.ReaderThreads.Remove(Thread.CurrentThread);
                Monitor.PulseAll(_lock);
            }
        }

        public void CommitWrite(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                if (tran.RootChanged)
                    _header.SetTreeRoot(tree, tran.TreeRoot);
                foreach (var page in tran.FreedPages)
                    FreePage(page);
                CommitHeader();
                foreach (var page in tran.UsedPages.Values)
                {
                    if (page == null || !page.IsDirty)
                        continue;
                    _file.SetPage(page.PageNumber, page.Save());
                }
                tran.HasWriter = false;
                tran.WriterThread = null;
                Monitor.PulseAll(_lock);
            }
        }

        public void RollbackWrite(int tree)
        {
            lock (_lock)
            {
                var tran = _transactions[tree];
                foreach (var page in tran.UsedPages.Keys)
                    _cache.Remove(page);
                foreach (var page in tran.AllocatedPages)
                    FreePage(page);
                CommitHeader();
                tran.HasWriter = false;
                tran.WriterThread = null;
                Monitor.PulseAll(_lock);
            }
        }

        public IdxOverflow GetOverflow(int tree, int page)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                IIdxPage loadedPage;
                var tran = _transactions[tree];
                if (!tran.HasWriter)
                {
                    return ReadOverflow(page);
                }
                else if (tran.UsedPages.TryGetValue(page, out loadedPage))
                    return loadedPage as IdxOverflow;
                else
                {
                    var overflow = ReadOverflow(page);
                    tran.UsedPages[page] = overflow;
                    return overflow;
                }
            }
        }

        private IdxOverflow ReadOverflow(int page)
        {
            CachedPage cachedPage;
            if (_cache.TryGetValue(page, out cachedPage))
                return cachedPage.UseAs<IdxOverflow>();
            var overflow = new IdxOverflow(_file.GetPage(page), _pageSize);
            overflow.PageNumber = page;
            _cache[page] = new CachedPage(overflow);
            return overflow;
        }

        public IdxOverflow CreateOverflow(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                var overflow = new IdxOverflow(null, _pageSize);
                var page = AllocPage();
                tran.AllocatedPages.Add(page);
                overflow.PageNumber = page;
                tran.UsedPages[overflow.PageNumber] = overflow;
                return overflow;
            }
        }

        public IIdxNode GetNode(int tree, int page)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                IIdxPage usedPage;
                if (!tran.HasWriter)
                    return ReadNode(page);
                else if (tran.UsedPages.TryGetValue(page, out usedPage))
                    return usedPage as IIdxNode;
                else
                {
                    var node = ReadNode(page);
                    tran.UsedPages[page] = node;
                    return node;
                }
            }
        }

        public IdxLeaf CreateLeaf(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                int page = AllocPage();
                tran.AllocatedPages.Add(page);
                var leaf = new IdxLeaf(null, _pageSize);
                leaf.PageNumber = page;
                tran.UsedPages[leaf.PageNumber] = leaf;
                return leaf;
            }
        }

        public IdxInterior CreateInterior(int tree)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                int page = AllocPage();
                tran.AllocatedPages.Add(page);
                var node = new IdxInterior(null, _pageSize);
                node.PageNumber = page;
                tran.UsedPages[node.PageNumber] = node;
                return node;
            }
        }

        public void Delete(int tree, int page)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                tran.FreedPages.Add(page);
                tran.UsedPages[page] = null;
                _cache.Remove(page);
            }
        }

        public void SetTreeRoot(int tree, IIdxNode root)
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                var tran = _transactions[tree];
                tran.TreeRoot = root.PageNumber;
                tran.RootChanged = true;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _timer.Dispose();
                _file.Dispose();
                _disposed = true;
                Monitor.PulseAll(_lock);
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("IdxContainer");
        }
    }
}
