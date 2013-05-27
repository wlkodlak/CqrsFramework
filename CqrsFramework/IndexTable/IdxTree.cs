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
        private int _pageSize;

        public IdxTree(IIdxContainer container, int tree)
        {
            _container = container;
            _tree = tree;
            _pageSize = _container.GetPageSize();
        }

        public void Insert(IdxKey key, byte[] value)
        {
            var root = _container.WriteTree(_tree);
            if (root == null)
            {
                var leaf = _container.CreateLeaf(_tree);
                var cell = CreateLeafCell(key, value);
                leaf.AddCell(cell);
                _container.SetTreeRoot(_tree, leaf);
            }
            else
            {
                var path = CreatePathToKey(key, root);
                var leaf = (IdxLeaf)path.GetCurrentNode();
                var cell = CreateLeafCell(key, value);
                if (!leaf.IsFull)
                    leaf.AddCell(cell);
                else
                {
                    var leaf2 = _container.CreateLeaf(_tree);
                    var splitKey = leaf.Split(leaf2, cell);
                    SplitInteriors(path, splitKey, leaf, leaf2);
                }
            }
            _container.CommitWrite(_tree);
        }

        private void SplitInteriors(Path path, IdxKey key, IIdxNode left, IIdxNode right)
        {
            while (right != null)
            {
                var parentNode = path.GetParentNode();
                var interiorCell = IdxCell.CreateInteriorCell(key, right.PageNumber, _pageSize);
                if (parentNode == null)
                {
                    var rootNode = _container.CreateInterior(_tree);
                    rootNode.LeftmostPage = left.PageNumber;
                    rootNode.AddCell(interiorCell);
                    _container.SetTreeRoot(_tree, rootNode);
                    right = null;
                }
                else
                {
                    path.GoUp();
                    right = null;
                    if (!parentNode.IsFull)
                        parentNode.AddCell(interiorCell);
                    else
                    {
                        var sibling = _container.CreateInterior(_tree);
                        key = parentNode.Split(sibling, interiorCell);
                        left = parentNode;
                        right = sibling;
                    }
                }
            }
        }

        private IdxCell CreateLeafCell(IdxKey key, byte[] value)
        {
            var cell = IdxCell.CreateLeafCell(key, value, _pageSize);
            if (cell.OverflowLength > 0)
            {
                IdxOverflow previousOverflow = null;
                var needsOverflow = true;
                var offset = cell.ValueLength;
                while (needsOverflow)
                {
                    var overflow = _container.CreateOverflow(_tree);
                    var written = overflow.WriteData(value, offset);
                    offset += written;
                    needsOverflow = overflow.NeedsNextPage;
                    if (previousOverflow == null)
                        cell.OverflowPage = overflow.PageNumber;
                    else
                        previousOverflow.Next = overflow.PageNumber;
                    previousOverflow = overflow;
                }
            }
            return cell;
        }

        public void Update(IdxKey key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Delete(IdxKey key)
        {
            var root = _container.WriteTree(_tree);
            var path = CreatePathToKey(key, root);
            var leafElement = path.GetCurrent();
            if (leafElement.ExactMatch)
            {
                leafElement.Leaf.RemoveCell(leafElement.CellIndex);
                MergeLeaves(path);
            }
            _container.CommitWrite(_tree);
        }

        private void MergeLeaves(Path path)
        {
            var leafElement = path.GetCurrent();
            var parentElement = path.GetParent();
            if (leafElement.Leaf.IsSmall && parentElement != null)
            {
                var leftNeighbourPage = path.GetNeighbourPage(-1);
                var rightNeighbourPage = path.GetNeighbourPage(1);
                if (rightNeighbourPage != 0)
                {
                    var rightNeighbourNode = (IdxLeaf)_container.GetNode(_tree, rightNeighbourPage);
                    var mergeKey = leafElement.Leaf.Merge(rightNeighbourNode);
                    parentElement.Interior.RemoveCell(parentElement.CellIndex + 1);
                    if (mergeKey != null)
                        parentElement.Interior.AddCell(IdxCell.CreateInteriorCell(mergeKey, rightNeighbourPage, _pageSize));
                    else
                    {
                        _container.Delete(_tree, rightNeighbourPage);
                        path.GoUp();
                        MergeInteriors(path);
                    }
                }
                else if (leftNeighbourPage != 0)
                {
                    var leftNeighbourNode = (IdxLeaf)_container.GetNode(_tree, leftNeighbourPage);
                    var mergeKey = leftNeighbourNode.Merge(leafElement.Leaf);
                    parentElement.Interior.RemoveCell(parentElement.CellIndex);
                    var thisPage = leafElement.Leaf.PageNumber;
                    if (mergeKey != null)
                        parentElement.Interior.AddCell(IdxCell.CreateInteriorCell(mergeKey, thisPage, _pageSize));
                    else
                    {
                        _container.Delete(_tree, thisPage);
                        path.GoUp();
                        MergeInteriors(path);
                    }
                }
            }
        }

        private void MergeInteriors(Path path)
        {
            var thisNode = (IdxInterior)path.GetCurrentNode();
            var parentElement = path.GetParent();
            var parentNode = parentElement == null ? null : parentElement.Interior;
            while (thisNode != null && thisNode.IsSmall && parentNode != null)
            {
                var leftPage = path.GetNeighbourPage(-1);
                var rightPage = path.GetNeighbourPage(1);
                if (rightPage != 0)
                {
                    var rightNode = (IdxInterior)_container.GetNode(_tree, rightPage);
                    var parentCell = parentNode.GetCell(parentElement.CellIndex + 1);
                    var mergeKey = thisNode.Merge(rightNode, parentCell);
                    parentNode.RemoveCell(parentElement.CellIndex + 1);
                    if (mergeKey != null)
                        parentNode.AddCell(IdxCell.CreateInteriorCell(mergeKey, rightPage, _pageSize));
                    else
                        _container.Delete(_tree, rightPage);
                }
                else if (leftPage != 0)
                {
                    var leftNode = (IdxInterior)_container.GetNode(_tree, leftPage);
                    var parentCell = parentNode.GetCell(parentElement.CellIndex);
                    var mergeKey = leftNode.Merge(thisNode, parentCell);
                    var thisPage = thisNode.PageNumber;
                    parentNode.RemoveCell(parentElement.CellIndex);
                    if (mergeKey != null)
                        parentNode.AddCell(IdxCell.CreateInteriorCell(mergeKey, thisPage, _pageSize));
                    else
                        _container.Delete(_tree, thisPage);
                }

                if (path.ParentIsRoot() && parentNode.CellsCount == 0)
                {
                    _container.Delete(_tree, parentNode.PageNumber);
                    _container.SetTreeRoot(_tree, thisNode);
                }
                else
                {
                    path.GoUp();
                    thisNode = parentNode;
                    parentElement = path.GetParent();
                    parentNode = parentElement == null ? null : parentElement.Interior;
                }
            }
        }

        public IEnumerable<KeyValuePair<IdxKey, byte[]>> Select(IdxKey min, IdxKey max)
        {
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            var node = _container.ReadTree(_tree);
            if (node == null)
                return result;
            try
            {
                var leafElement = CreatePathToKey(min, node).GetCurrent();
                var leaf = leafElement.Leaf;
                var cellIndex = leafElement.ExactMatch ? leafElement.CellIndex : leafElement.CellIndex + 1;

                if (cellIndex >= leaf.CellsCount)
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

        private class Path
        {
            private List<PathElement> _path = new List<PathElement>();

            public PathElement GetCurrent()
            {
                return (_path.Count == 0) ? null : _path[_path.Count - 1];
            }

            public IIdxNode GetCurrentNode()
            {
                var current = GetCurrent();
                if (current == null)
                    return null;
                else if (current.Leaf != null)
                    return current.Leaf;
                else
                    return current.Interior;
            }

            public void AddInterior(IdxInterior interior, int cellIndex, bool exact)
            {
                _path.Add(new PathElement { Interior = interior, CellIndex = cellIndex, ExactMatch = exact });
            }

            public void AddLeaf(IdxLeaf leaf, int cellIndex, bool exact)
            {
                _path.Add(new PathElement { Leaf = leaf, CellIndex = cellIndex, ExactMatch = exact });
            }

            public PathElement GetParent()
            {
                return (_path.Count < 2) ? null : _path[_path.Count - 2];
            }

            public IdxInterior GetParentNode()
            {
                var parentElement = GetParent();
                return parentElement == null ? null : parentElement.Interior;
            }

            public int GetNeighbourPage(int offset)
            {
                var parentElement = GetParent();
                if (parentElement == null)
                    return 0;
                var parentCellsCount = parentElement.Interior.CellsCount;
                var wantedCellIndex = parentElement.CellIndex + offset;
                if (wantedCellIndex < -1)
                    return 0;
                else if (wantedCellIndex == -1)
                    return parentElement.Interior.LeftmostPage;
                else if (wantedCellIndex < parentCellsCount)
                    return parentElement.Interior.GetCell(wantedCellIndex).ChildPage;
                else
                    return 0;
            }

            public void GoUp()
            {
                _path.RemoveAt(_path.Count - 1);
            }

            public bool ParentIsRoot()
            {
                return _path.Count <= 2;
            }
        }

        private class PathElement
        {
            public int CellIndex;
            public IdxLeaf Leaf;
            public IdxInterior Interior;
            public bool ExactMatch;
        }

        private Path CreatePathToKey(IdxKey key, IIdxNode node)
        {
            var path = new Path();

            while (!node.IsLeaf)
            {
                var interior = (IdxInterior)node;
                IdxCell foundCell = FindCellForKey(key, node);

                int childPage;
                if (foundCell == null)
                {
                    path.AddInterior(interior, -1, false);
                    childPage = interior.LeftmostPage;
                }
                else
                {
                    path.AddInterior(interior, foundCell.Ordinal, foundCell.Key == key);
                    childPage = foundCell.ChildPage;
                }
                if (childPage == 0)
                    throw new NullReferenceException(string.Format("Null child page in node {0}", node.PageNumber));
                node = _container.GetNode(_tree, childPage);
            }

            {
                var foundCell = FindCellForKey(key, node);
                if (foundCell == null)
                    path.AddLeaf((IdxLeaf)node, -1, false);
                else
                    path.AddLeaf((IdxLeaf)node, foundCell.Ordinal, foundCell.Key == key);
            }

            return path;
        }

        private IdxCell FindCellForKey(IdxKey key, IIdxNode node)
        {
            IdxCell foundCell = null;
            IdxCell lastCell = null;
            for (int i = 0; i <= node.CellsCount; i++)
            {
                if (i == node.CellsCount)
                {
                    foundCell = lastCell;
                    break;
                }
                var cell = node.GetCell(i);
                if (cell.Key == key)
                {
                    foundCell = cell;
                    break;
                }
                else if (cell.Key > key)
                {
                    foundCell = lastCell;
                    break;
                }
                lastCell = cell;
            }
            return foundCell;
        }

        private byte[] ReadCellValue(IdxCell cell)
        {
            if (cell.OverflowPage == 0)
                return cell.ValueBytes;
            var buffer = new byte[IdxOverflow.Capacity(_pageSize) * cell.OverflowLength + cell.ValueLength];
            Array.Copy(cell.ValueBytes, buffer, cell.ValueLength);
            var offset = cell.ValueLength;
            var overflowPage = cell.OverflowPage;
            while (overflowPage != 0)
            {
                var overflow = _container.GetOverflow(_tree, overflowPage);
                var read = overflow.ReadData(buffer, offset);
                offset += read;
                overflowPage = overflow.Next;
            }
            Array.Resize(ref buffer, offset);
            return buffer;
        }
    }
}
