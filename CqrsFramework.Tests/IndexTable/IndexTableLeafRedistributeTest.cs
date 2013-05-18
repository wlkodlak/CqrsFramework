using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    public abstract class IndexTableLeafRedistributeTestBase
    {
        private IdxLeaf _leftNode, _rightNode;

        protected abstract IdxLeaf CreateLeftNode();
        protected abstract IdxLeaf CreateRightNode();

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateLeftNode();
            _rightNode = CreateRightNode();
        }

        [TestMethod]
        public void AreMergeable()
        {
            var leftMaxKey = _leftNode.GetCell(_leftNode.CellsCount - 1).Key;
            var rightMinKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(_leftNode.IsSmall || _rightNode.IsSmall, "Either small");
            Assert.IsTrue(_leftNode.IsSmall != _rightNode.IsSmall, "Only one of them can be small");
            Assert.IsTrue(leftMaxKey < rightMinKey, "Left max key is less than right min key: {0} < {1}", leftMaxKey, rightMinKey);
            Assert.AreEqual(_rightNode.PageNumber, _leftNode.NextLeaf);
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
            _leftNode.Merge(_rightNode);
            var newSum = _leftNode.CellsCount + _rightNode.CellsCount;
            Assert.AreEqual(originalSum, newSum);
        }

        [TestMethod]
        public void LeftKeysAreLessThanReturnedKey()
        {
            var key = _leftNode.Merge(_rightNode);
            for (int i = 0; i < _leftNode.CellsCount; i++)
            {
                var cellKey = _leftNode.GetCell(i).Key;
                Assert.IsTrue(cellKey < key, "Cell {0} key {1} < returned key {2}", i, cellKey, key);
            }
        }

        [TestMethod]
        public void ResultingNodesHaveSimilarSize()
        {
            var key = _leftNode.Merge(_rightNode);
            var leftSize = Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).Sum(c => c.CellSize);
            var rightSize = Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).Sum(c => c.CellSize);
            var difference = Math.Abs(leftSize - rightSize);
            Assert.IsTrue(difference <= 128);
        }

        [TestMethod]
        public void RightKeysAreMoreOrEqualReturnedKey()
        {
            var key = _leftNode.Merge(_rightNode);
            for (int i = 0; i < _rightNode.CellsCount; i++)
            {
                var cellKey = _rightNode.GetCell(i).Key;
                Assert.IsTrue(cellKey >= key, "Cell {0} key {1} >= returned key {2}", i, cellKey, key);
            }
        }

        [TestMethod]
        public void LeftNodeOrdered()
        {
            _leftNode.Merge(_rightNode);
            AssertNodeOrdered(_leftNode);
        }

        [TestMethod]
        public void RightNodeOrdered()
        {
            _leftNode.Merge(_rightNode);
            AssertNodeOrdered(_rightNode);
        }

        private static void AssertNodeOrdered(IdxLeaf node)
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
            _leftNode.Merge(_rightNode);
            Assert.IsFalse(_leftNode.IsSmall, "Left small");
            Assert.IsFalse(_rightNode.IsSmall, "Right small");
            Assert.IsFalse(_leftNode.IsFull, "Left full");
            Assert.IsFalse(_rightNode.IsFull, "Right full");
        }

        public static void AddNodeCell(IdxLeaf node, int keyBase, int cellLength)
        {
            var key = IdxKey.FromInteger(keyBase);
            var cellData = new byte[cellLength - 12];
            var cell = IdxCell.CreateLeafCell(key, cellData);
            node.AddCell(cell);
        }
    }

    [TestClass]
    public class IndexTableLeafRedistributeTestToLeftWithFull : IndexTableLeafRedistributeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1931;
            node.NextLeaf = 1932;
            for (int i = 0; i < 55; i++)
                AddNodeCell(node, i * 10, 16);
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1932;
            node.NextLeaf = 1933;
            for (int i = 300; i < 1024; i++)
                if (!node.IsFull)
                    AddNodeCell(node, i * 10, 16);
            return node;
        }

    }

    [TestClass]
    public class IndexTableLeafRedistributeTestToLeftWithBorderline : IndexTableLeafRedistributeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1931;
            node.NextLeaf = 1932;
            for (int i = 0; i < 59; i++)
                AddNodeCell(node, i * 10, 16);
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1932;
            node.NextLeaf = 1933;
            for (int i = 300; i < 367; i++)
                AddNodeCell(node, i * 10, 16);
            return node;
        }
    }

    [TestClass]
    public class IndexTableLeafRedistributeTestToRightWithBorderline : IndexTableLeafRedistributeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1931;
            node.NextLeaf = 1932;
            for (int i = 0; i < 63; i++)
                AddNodeCell(node, i * 10, 16);
            AddNodeCell(node, 64, 128);
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            var node = new IdxLeaf(null);
            node.PageNumber = 1932;
            node.NextLeaf = 1933;
            for (int i = 300; i < 355; i++)
                AddNodeCell(node, i * 10, 16);
            return node;
        }
    }
}
