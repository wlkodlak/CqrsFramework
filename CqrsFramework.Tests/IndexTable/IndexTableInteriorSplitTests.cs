using System;
using System.Linq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace CqrsFramework.Tests.IndexTable
{
    public class IndexTableInteriorSplitTestsBase
    {
        private int _position;
        private IdxInterior _leftNode, _rightNode;
        private IdxCell _addedCell;

        protected IndexTableInteriorSplitTestsBase(int position)
        {
            _position = position;
        }

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateNodeForSplit();
            _leftNode.PageNumber = 1001;
            _rightNode = new IdxInterior(null);
            _rightNode.PageNumber = 1002;
            _addedCell = CreateAddedCell();
        }

        protected virtual IdxInterior CreateNodeForSplit()
        {
            var node = new IdxInterior(null);
            node.LeftmostPage = 8175;

            for (int i = 0; i < 64; i++)
                node.AddCell(MakeCell(i * 10, 4, i * 13));
            for (int i = 64; i < 96; i++)
                node.AddCell(MakeCell(i * 10, 56, i * 13));
            for (int i = 96; i < 256; i++)
            {
                if (!node.IsFull)
                    node.AddCell(MakeCell(i * 10, 120, i * 13));
            }
            return node;
        }

        protected virtual IdxCell CreateAddedCell()
        {
            return MakeCell(10 * _position + 5, 80, _position * 17);
        }

        protected static IdxCell MakeCell(int keyBase, int keyLength, int page)
        {
            var keyBytes = new byte[keyLength];
            keyBytes[0] = (byte)((keyBase >> 24) & 0xFF);
            keyBytes[1] = (byte)((keyBase >> 16) & 0xFF);
            keyBytes[2] = (byte)((keyBase >>  8) & 0xFF);
            keyBytes[3] = (byte)(keyBase & 0xFF);
            return IdxCell.CreateInteriorCell(IdxKey.FromBytes(keyBytes), page);
        }

        [TestMethod]
        public void CorrectStart()
        {
            Assert.IsTrue(_leftNode.IsFull, "Left full");
            Assert.AreEqual(0, _rightNode.CellsCount, "Right empty");
        }

        [TestMethod]
        public void TotalCountsAreEqual()
        {
            int originalCount = _leftNode.CellsCount;
            _leftNode.Split(_rightNode, _addedCell);
            int resultingSum = _leftNode.CellsCount + _rightNode.CellsCount;
            Assert.AreEqual(originalCount, resultingSum);
        }

        private static void AssertNodeIsSorted(IdxInterior node)
        {
            Assert.AreEqual(0, node.GetCell(0).Ordinal, "Cell {0} ordinal", 0);
            for (int i = 1; i < node.CellsCount; i++)
            {
                var currentCell = node.GetCell(i);
                var currentKey = currentCell.Key;
                var previousKey = node.GetCell(i - 1).Key;
                Assert.AreEqual(i, node.GetCell(i).Ordinal, "Cell {0} ordinal", i);
                Assert.IsTrue(previousKey < currentKey,
                    "Cell {0} key has to be less than cell {1} key: {2} < {3}",
                    i - 1, i, previousKey, currentKey);
            }
        }

        [TestMethod]
        public void LeftNodeSorted()
        {
            _leftNode.Split(_rightNode, _addedCell);
            AssertNodeIsSorted(_leftNode);
        }

        [TestMethod]
        public void RightNodeSorted()
        {
            _leftNode.Split(_rightNode, _addedCell);
            AssertNodeIsSorted(_rightNode);
        }

        [TestMethod]
        public void LeftKeysLessThanKey()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            var lastLeftKey = _leftNode.GetCell(_leftNode.CellsCount - 1).Key;
            Assert.IsTrue(lastLeftKey < key);
        }

        [TestMethod]
        public void RightKeysGreaterThanKey()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            var firstRightKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(key < firstRightKey);
        }

        [TestMethod]
        public void NeigherIsSmallNorFull()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            Assert.IsFalse(_leftNode.IsSmall, "Left small");
            Assert.IsFalse(_leftNode.IsFull, "Left full");
            Assert.IsFalse(_rightNode.IsSmall, "Right small");
            Assert.IsFalse(_rightNode.IsFull, "Right full");
        }

        [TestMethod]
        public void BothNodesAreAboutSameSize()
        {
            _leftNode.Split(_rightNode, _addedCell);
            int leftSize = Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).Sum(c => c.CellSize);
            int rightSize = Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).Sum(c => c.CellSize);
            Assert.IsTrue(Math.Abs(leftSize - rightSize) <= 128);
        }

        [TestMethod]
        public void LeftmostPageOfRightTree()
        {
            var originalCells = Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).ToDictionary(c => c.Key);
            var key = _leftNode.Split(_rightNode, _addedCell);
            int expectedPage;
            if (key == _addedCell.Key)
                expectedPage = _addedCell.ChildPage;
            else
                expectedPage = originalCells[key].ChildPage;
            Assert.AreEqual(expectedPage, _rightNode.LeftmostPage);
        }
    }

    [TestClass]
    public class IndexTableInteriorSplitA : IndexTableInteriorSplitTestsBase
    {
        public IndexTableInteriorSplitA()
            : base(0)
        {
        }
    }

    [TestClass]
    public class IndexTableInteriorSplitB : IndexTableInteriorSplitTestsBase
    {
        public IndexTableInteriorSplitB()
            : base(104)
        {
        }
    }

    [TestClass]
    public class IndexTableInteriorSplitC : IndexTableInteriorSplitTestsBase
    {
        public IndexTableInteriorSplitC()
            : base(78)
        {
        }
    }

    [TestClass]
    public class IndexTableInteriorSplitR : IndexTableInteriorSplitTestsBase
    {
        private Random _random;
        private HashSet<IdxKey> _usedKeys;

        public IndexTableInteriorSplitR()
            : base(80)
        {
            _random = new Random(847245);
            _usedKeys = new HashSet<IdxKey>();
        }

        private IdxCell TotallyRandomCell()
        {
            while (true)
            {
                var keyLength = _random.Next() % 120;
                var bytes = new byte[keyLength];
                _random.NextBytes(bytes);
                var key = IdxKey.FromBytes(bytes);
                if (_usedKeys.Add(key))
                    return IdxCell.CreateInteriorCell(key, _random.Next());
            }
        }

        protected override IdxCell CreateAddedCell()
        {
            return TotallyRandomCell();
        }

        protected override IdxInterior CreateNodeForSplit()
        {
            IdxInterior node = new IdxInterior(null);
            for (int i = 0; i < 1024 && !node.IsFull; i++)
                node.AddCell(TotallyRandomCell());
            return node;
        }
    }
}
