using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    public class TestTreeBuilder
    {
        private List<TestTreeNodeBuilder> _nodeBuilders = new List<TestTreeNodeBuilder>();
        private Random _random = new Random(48324);
        private Dictionary<string, byte[]> _namedValues = new Dictionary<string, byte[]>();

        private Dictionary<int, IIdxPage> _pagesIndex = new Dictionary<int, IIdxPage>();
        private List<IIdxPage> _pages = new List<IIdxPage>();

        public void Build()
        {
            foreach (var node in _nodeBuilders.ToList())
            {
                if (node.IsLeaf)
                    PrepareLeafNode(node);
                else
                    PrepareInteriorNode(node);
            }
            var roots = new List<TestTreeNodeBuilder>();
            foreach (var node in _nodeBuilders.ToList())
            {
                AllocPageNumbersForNode(node);
                if (node.ParentNode == null)
                    roots.Add(node);
            }
            foreach (var root in roots)
                root.DistributeNext(null);
            foreach (var node in _nodeBuilders.ToList())
            {
                if (node.IsLeaf)
                    FinishLeafPage(node);
                else
                    FinishInteriorPage(node);
            }
            foreach (var page in _pages)
                _pagesIndex[page.PageNumber] = page;
        }

        private void FinishLeafPage(TestTreeNodeBuilder node)
        {
            node.LeafPage.NextLeaf = node.NextNode == null ? 0 : node.NextNode.PageNumber;
            foreach (var cell in node.Cells)
                node.LeafPage.AddCell(cell.Cell);
        }

        private void FinishInteriorPage(TestTreeNodeBuilder node)
        {
            if (node.Leftmost != null)
                node.InteriorPage.LeftmostPage = node.Leftmost.PageNumber;
            foreach (var cell in node.Cells)
            {
                var page = cell.Node == null ? AllocPage() : cell.Node.PageNumber;
                cell.Cell = IdxCell.CreateInteriorCell(cell.Key, page, PageSize);
                node.InteriorPage.AddCell(cell.Cell);
            }
        }

        private void AllocPageNumbersForNode(TestTreeNodeBuilder node)
        {
            if (node.PageNumber == 0)
                node.PageNumber = AllocPage();
            if (node.IsLeaf)
            {
                node.LeafPage.PageNumber = node.PageNumber;
                foreach (var cell in node.Cells)
                {
                    for (int i = cell.OverflowPages.Count - 1; i >= 0; i--)
                    {
                        int page = cell.OverflowPages[i].PageNumber;
                        if (page == 0)
                            cell.OverflowPages[i].PageNumber = page = AllocPage();
                        if (i == 0)
                            cell.Cell.OverflowPage = page;
                        else
                            cell.OverflowPages[i - 1].Next = page;
                    }
                }
            }
            else
                node.InteriorPage.PageNumber = node.PageNumber;
        }

        private void PrepareLeafNode(TestTreeNodeBuilder node)
        {
            node.LeafPage = new IdxLeaf(null, PageSize);
            node.LeafPage.PageNumber = node.PageNumber;
            MarkAsUsed(node.PageNumber);
            _pages.Add(node.LeafPage);
            foreach (var cellBuilder in node.Cells)
            {
                PrepareLeafCell(cellBuilder);
            }
        }

        private void PrepareLeafCell(TestTreeCellBuilder cellBuilder)
        {
            cellBuilder.Cell = IdxCell.CreateLeafCell(cellBuilder.Key, cellBuilder.Value, PageSize);
            var offset = cellBuilder.Cell.ValueLength;
            var remaining = (cellBuilder.Value == null) ? 0 : cellBuilder.Value.Length - cellBuilder.Cell.ValueLength;
            while (remaining > 0)
            {
                var overflowPage = new IdxOverflow(null, PageSize);
                var written = overflowPage.WriteData(cellBuilder.Value, offset);
                offset += written;
                remaining -= written;
                cellBuilder.OverflowPages.Add(overflowPage);
                _pages.Add(overflowPage);
            }
            if (cellBuilder.OverflowPageNumbers != null)
            {
                for (int i = cellBuilder.OverflowPages.Count; i < cellBuilder.OverflowPageNumbers.Length; i++)
                {
                    var overflowPage = new IdxOverflow(null, PageSize);
                    _pages.Add(overflowPage);
                    cellBuilder.OverflowPages.Add(overflowPage);
                }
                for (int i = 0; i < cellBuilder.OverflowPageNumbers.Length; i++)
                {
                    cellBuilder.OverflowPages[i].PageNumber = cellBuilder.OverflowPageNumbers[i];
                    MarkAsUsed(cellBuilder.OverflowPageNumbers[i]);
                }
            }
        }

        private void PrepareInteriorNode(TestTreeNodeBuilder node)
        {
            node.InteriorPage = new IdxInterior(null, PageSize);
            node.InteriorPage.PageNumber = node.PageNumber;
            MarkAsUsed(node.PageNumber);
            _pages.Add(node.InteriorPage);
            if (node.Leftmost == null)
                node.Leftmost = BuildChildNodeForInterior(node);
            node.Leftmost.ParentNode = node;
            foreach (var cellBuilder in node.Cells)
            {
                if (cellBuilder.Node == null)
                    cellBuilder.Node = BuildChildNodeForInterior(node);
                cellBuilder.Node.ParentNode = node;
            }
        }

        private TestTreeNodeBuilder BuildChildNodeForInterior(TestTreeNodeBuilder node)
        {
            var newNode = new TestTreeNodeBuilder(this);
            newNode.IsLeaf = node.ContentsType != 1;
            _nodeBuilders.Add(newNode);
            if (newNode.IsLeaf)
                PrepareLeafNode(newNode);
            else
                PrepareInteriorNode(newNode);
            return newNode;
        }

        public int PageSize;
        public int MinKeySize;

        private int _pageNumber = 2;
        private List<int> _usablePages = new List<int>();

        private void MarkAsUsed(int page)
        {
            if (page == 0)
                return;
            if (page < _pageNumber)
                _usablePages.Remove(page);
            else if (page == _pageNumber)
                _pageNumber++;
            else
            {
                for (int i = _pageNumber; i < page; i++)
                    _usablePages.Add(i);
                _pageNumber = page + 1;
            }
        }

        private int AllocPage()
        {
            if (_usablePages.Count == 0)
            {
                int page = _pageNumber;
                _pageNumber++;
                return page;
            }
            else
            {
                int index = _usablePages.Count - 1;
                int page = _usablePages[index];
                _usablePages.RemoveAt(index);
                return page;
            }
        }

        public TestTreeNodeBuilder Interior(int pageNumber)
        {
            var node = new TestTreeNodeBuilder(this);
            node.IsLeaf = false;
            node.PageNumber = pageNumber;
            _nodeBuilders.Add(node);
            return node;
        }

        public TestTreeNodeBuilder Leaf(int pageNumber)
        {
            var node = new TestTreeNodeBuilder(this);
            node.IsLeaf = true;
            node.PageNumber = pageNumber;
            _nodeBuilders.Add(node);
            return node;
        }

        public void SetNamedValue(string name, byte[] value)
        {
            _namedValues[name] = value;
        }

        public byte[] GetNamedValue(string name)
        {
            byte[] output;
            if (_namedValues.TryGetValue(name, out output))
                return output;
            else
                return new byte[0];
        }

        public IIdxNode GetNode(int page)
        {
            IIdxPage contents;
            if (!_pagesIndex.TryGetValue(page, out contents))
                throw new KeyNotFoundException(string.Format("Page {0} not found", page));
            return (IIdxNode)contents;
        }

        public IdxOverflow GetOverflow(int page)
        {
            IIdxPage contents;
            if (!_pagesIndex.TryGetValue(page, out contents))
                throw new KeyNotFoundException(string.Format("Page {0} not found", page));
            return (IdxOverflow)contents;
        }

        public byte[] CreateValue(int length)
        {
            byte[] value = new byte[length];
            _random.NextBytes(value);
            return value;
        }

        public IdxKey BuildKey(int keyBase)
        {
            var intKey = IdxKey.FromInteger(keyBase);
            var intKeyBytes = intKey.ToBytes();
            if (MinKeySize <= intKeyBytes.Length)
                return intKey;
            var longKey = new byte[MinKeySize];
            Array.Copy(intKeyBytes, longKey, intKeyBytes.Length);
            return IdxKey.FromBytes(longKey);
        }

        public TestTreeCellBuilder LongCell(int keyBase, byte[] value, params int[] overflowPages)
        {
            var cell = new TestTreeCellBuilder();
            cell.Key = BuildKey(keyBase);
            cell.Value = value;
            cell.OverflowPageNumbers = overflowPages;
            return cell;
        }
    }

    public class TestTreeNodeBuilder
    {
        private TestTreeBuilder _builder;
        public bool IsLeaf;
        public int PageNumber;
        public TestTreeNodeBuilder Leftmost;
        public List<TestTreeCellBuilder> Cells = new List<TestTreeCellBuilder>();
        private TestTreeCellBuilder _currentCell;
        public IdxLeaf LeafPage;
        public IdxInterior InteriorPage;
        public TestTreeNodeBuilder ParentNode;
        public TestTreeNodeBuilder NextNode;
        public int ContentsType = 0;

        public TestTreeNodeBuilder(TestTreeBuilder builder)
        {
            _builder = builder;
        }

        public void MarkAsAboveLeaves()
        {
            ContentsType = 2;
        }

        public void MarkAsAboveInteriors()
        {
            ContentsType = 1;
        }
        
        public void AddContents(params object[] contents)
        {
            if (!IsLeaf)
            {
                foreach (object member in contents)
                {
                    if (member is TestTreeNodeBuilder)
                    {
                        var nodeBuilder = member as TestTreeNodeBuilder;
                        if (_currentCell == null)
                            Leftmost = nodeBuilder;
                        else if (_currentCell.Node == null)
                            _currentCell.Node = nodeBuilder;
                        ContentsType = nodeBuilder.IsLeaf ? 2 : 1;
                    }
                    else if (member is int)
                    {
                        _currentCell = new TestTreeCellBuilder();
                        _currentCell.Key = _builder.BuildKey((int)member);
                        _currentCell.IsLeaf = false;
                        Cells.Add(_currentCell);
                    }
                }
            }
            else
            {
                foreach (object member in contents)
                {
                    if (member is int)
                    {
                        _currentCell = new TestTreeCellBuilder();
                        _currentCell.Key = _builder.BuildKey((int)member);
                        _currentCell.IsLeaf = true;
                        Cells.Add(_currentCell);
                    }
                    else if (member is byte[])
                        _currentCell.Value = member as byte[];
                    else if (member is TestTreeCellBuilder)
                    {
                        _currentCell = member as TestTreeCellBuilder;
                        Cells.Add(_currentCell);
                    }
                }
            }
        }

        public TestTreeNodeBuilder DistributeNext(TestTreeNodeBuilder nextNode)
        {
            if (IsLeaf)
            {
                NextNode = nextNode;
                return this;
            }
            else
            {
                for (int i = Cells.Count - 1; i >= 0; i--)
                    nextNode = Cells[i].Node.DistributeNext(nextNode);
                return nextNode;
            }
        }
    }

    public class TestTreeCellBuilder
    {
        public IdxKey Key;
        public bool IsLeaf;
        public TestTreeNodeBuilder Node;
        public byte[] Value;
        public int[] OverflowPageNumbers;
        public IdxCell Cell;
        public List<IdxOverflow> OverflowPages = new List<IdxOverflow>();
    }
}
