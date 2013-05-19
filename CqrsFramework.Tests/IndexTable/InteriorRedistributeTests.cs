using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    public abstract class InteriorRedistributeTestBase
    {
        private IdxInterior _leftNode, _rightNode;
        private IdxCell _parentCell;

        protected abstract IdxInterior CreateLeftNode();
        protected abstract IdxInterior CreateRightNode();
        protected abstract IdxCell CreatMiddleCell();

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateLeftNode();
            _rightNode = CreateRightNode();
            _parentCell = CreatMiddleCell();
        }

        [TestMethod]
        public void AreMergeable()
        {
            var leftMaxKey = _leftNode.GetCell(_leftNode.CellsCount - 1).Key;
            var rightMinKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(_leftNode.IsSmall || _rightNode.IsSmall, "Either small");
            Assert.IsTrue(_leftNode.IsSmall != _rightNode.IsSmall, "Only one of them can be small");
            Assert.IsTrue(leftMaxKey < rightMinKey, "Left max key is less than right min key: {0} < {1}", leftMaxKey, rightMinKey);
            Assert.AreEqual(_rightNode.PageNumber, _parentCell.ChildPage, "Parent cell page");
            if (!_leftNode.IsSmall)
            {
                for (int i = 0; i < 250 && _rightNode.IsSmall; i++)
                {
                    var leftIndex = _leftNode.CellsCount - 1;
                    var cell = _leftNode.GetCell(leftIndex);
                    _leftNode.RemoveCell(leftIndex);
                    _rightNode.AddCell(cell);
                }
                Assert.IsFalse(_leftNode.IsSmall, "Left small after moving");
                Assert.IsFalse(_rightNode.IsSmall, "Right small after moving");
            }
            if (!_rightNode.IsSmall)
            {
                for (int i = 0; i < 250 && _leftNode.IsSmall; i++)
                {
                    var cell = _rightNode.GetCell(0);
                    _rightNode.RemoveCell(0);
                    _leftNode.AddCell(cell);
                }
                Assert.IsFalse(_leftNode.IsSmall, "Left small after moving");
                Assert.IsFalse(_rightNode.IsSmall, "Right small after moving");
            }
        }

        [TestMethod]
        public void SumOfNodesConstant()
        {
            var originalSum = _leftNode.CellsCount + _rightNode.CellsCount;
            _leftNode.Merge(_rightNode, _parentCell);
            var newSum = _leftNode.CellsCount + _rightNode.CellsCount;
            Assert.AreEqual(originalSum, newSum);
        }

        [TestMethod]
        public void LeftKeysAreLessThanReturnedKey()
        {
            var key = _leftNode.Merge(_rightNode, _parentCell);
            for (int i = 0; i < _leftNode.CellsCount; i++)
            {
                var cellKey = _leftNode.GetCell(i).Key;
                Assert.IsTrue(cellKey < key, "Cell {0} key {1} < returned key {2}", i, cellKey, key);
            }
        }

        [TestMethod]
        public void ResultingNodesHaveSimilarSize()
        {
            var key = _leftNode.Merge(_rightNode, _parentCell);
            var leftSize = Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).Sum(c => c.CellSize);
            var rightSize = Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).Sum(c => c.CellSize);
            var difference = Math.Abs(leftSize - rightSize);
            Assert.IsTrue(difference <= 128);
        }

        [TestMethod]
        public void RightKeysAreGreaterThanReturnedKey()
        {
            var key = _leftNode.Merge(_rightNode, _parentCell);
            for (int i = 0; i < _rightNode.CellsCount; i++)
            {
                var cellKey = _rightNode.GetCell(i).Key;
                Assert.IsTrue(cellKey > key, "Cell {0} key {1} >= returned key {2}", i, cellKey, key);
            }
        }

        [TestMethod]
        public void LeftNodeOrdered()
        {
            _leftNode.Merge(_rightNode, _parentCell);
            AssertNodeOrdered(_leftNode);
        }

        [TestMethod]
        public void RightNodeOrdered()
        {
            _leftNode.Merge(_rightNode, _parentCell);
            AssertNodeOrdered(_rightNode);
        }

        private static void AssertNodeOrdered(IdxInterior node)
        {
            Assert.AreEqual(0, node.GetCell(0).Ordinal, "Cell 0 ordinal");
            for (int i = 1; i < node.CellsCount; i++)
            {
                var prevKey = node.GetCell(i - 1).Key;
                var currKey = node.GetCell(i).Key;
                var currOrd = node.GetCell(i).Ordinal;
                Assert.AreEqual(i, currOrd, "Cell {0} ordinal", i);
                Assert.IsTrue(prevKey < currKey, "Cell {0}'s previous key vs current key: {1} < {2}", i, prevKey, currKey);
            }
        }

        [TestMethod]
        public void NeitherIsSmallNorFull()
        {
            _leftNode.Merge(_rightNode, _parentCell);
            Assert.IsFalse(_leftNode.IsSmall, "Left small");
            Assert.IsFalse(_rightNode.IsSmall, "Right small");
            Assert.IsFalse(_leftNode.IsFull, "Left full");
            Assert.IsFalse(_rightNode.IsFull, "Right full");
        }

        [TestMethod]
        public void ParentCell()
        {
            var keyPages = AllCells(_leftNode).Union(AllCells(_rightNode)).ToDictionary(c => c.Key, c => c.ChildPage);
            var originalLeftMost = _rightNode.LeftmostPage;
            var key = _leftNode.Merge(_rightNode, _parentCell);
            if (key == _parentCell.Key)
            {
                Assert.IsFalse(AllCells(_leftNode).Any(c => c.Key == _parentCell.Key), "Parent cell key in left");
                Assert.IsFalse(AllCells(_leftNode).Any(c => c.Key == _parentCell.Key), "Parent cell key in right");
            }
            else
            {
                var nodeWithParent = (key > _parentCell.Key) ? _leftNode : _rightNode;
                var foundCell = AllCells(nodeWithParent).FirstOrDefault(c => c.Key == _parentCell.Key);
                Assert.IsNotNull(foundCell, "Parent key not found");
                Assert.AreEqual(originalLeftMost, foundCell.ChildPage, "Original leftmost page");
                Assert.AreEqual(keyPages[key], _rightNode.LeftmostPage, "New leftmost page");
            }
        }

        private IEnumerable<IdxCell> AllCells(IdxInterior node)
        {
            return Enumerable.Range(0, node.CellsCount).Select(i => node.GetCell(i));
        }

        public static IdxCell CreateCell(int keyBase, int page, int cellSize)
        {
            var bytes = new byte[cellSize - 8];
            bytes[0] = (byte)((keyBase >> 24) & 0xff);
            bytes[1] = (byte)((keyBase >> 16) & 0xff);
            bytes[2] = (byte)((keyBase >> 8) & 0xff);
            bytes[3] = (byte)(keyBase & 0xff);
            return IdxCell.CreateInteriorCell(IdxKey.FromBytes(bytes), page);
        }
    }

    [TestClass]
    public class InteriorRedistributeTestToLeftFull : InteriorRedistributeTestBase
    {
        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1;
            node.LeftmostPage = 2;
            for (int i = 0; i < 7; i++)
                node.AddCell(CreateCell(10 + i, 3 + i, 128));
            return node;
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 10;
            node.LeftmostPage = 20;
            for (int i = 0; i < 100; i++)
                if (!node.IsFull)
                    node.AddCell(CreateCell(100 + i, 103 + i, 128));
            return node;
        }

        protected override IdxCell CreatMiddleCell()
        {
            return CreateCell(80, 10, 128);
        }
    }

    [TestClass]
    public class InteriorRedistributeTestToRightBorderline : InteriorRedistributeTestBase
    {
        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1;
            node.LeftmostPage = 2;
            for (int i = 0; i < 63; i++)
                node.AddCell(CreateCell(10 + i, 3 + i, 16));
            node.AddCell(CreateCell(80, 293, 128));
            node.AddCell(CreateCell(81, 28, 32));
            return node;
        }

        protected override IdxCell CreatMiddleCell()
        {
            return CreateCell(90, 10, 16);
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 10;
            node.LeftmostPage = 20;
            for (int i = 0; i < 60; i++)
                node.AddCell(CreateCell(100 + i, 7 + i, 16));
            return node;
        }
    }
}