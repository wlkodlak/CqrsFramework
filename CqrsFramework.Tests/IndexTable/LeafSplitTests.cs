using System;
using System.Linq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    public abstract class LeafSplitTestBase
    {
        private int _position;
        private IdxLeaf _leftNode;
        private IdxLeaf _rightNode;
        private IdxCell _addedCell;

        public LeafSplitTestBase(int position)
        {
            this._position = position;
        }

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateForSplit();
            _leftNode.PageNumber = 1222;
            _rightNode = new IdxLeaf(null);
            _rightNode.PageNumber = 2344;
            _addedCell = CreateCell(10 * _position + 5, 16);
        }

        public static IdxCell CreateCell(int index, int size)
        {
            var valueBytes = new byte[size - 12];
            new Random(index * 548).NextBytes(valueBytes);
            return IdxCell.CreateLeafCell(IdxKey.FromInteger(index), valueBytes);
        }

        public static IdxLeaf CreateForSplit()
        {
            var node = new IdxLeaf(null);
            node.NextLeaf = 8432;
            for (int i = 0; i < 64; i++)
                node.AddCell(CreateCell(i * 10, 16));
            for (int i = 0; i < 16; i++)
                node.AddCell(CreateCell(i * 10 + 640, 64));
            for (int i = 0; i < 8; i++)
                node.AddCell(CreateCell(i * 10 + 800, 128));
            for (int i = 0; i < 1024; i++)
                if (!node.IsFull)
                    node.AddCell(CreateCell(i * 10 + 880, 16));
            return node;
        }

        [TestMethod]
        public void OriginalNodeIsFull()
        {
            Assert.IsTrue(CreateForSplit().IsFull);
        }

        [TestMethod]
        public void CollectionIsSplitRoughlyInHalf()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            var leftSize = Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).Sum(c => c.CellSize);
            var rightSize = Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).Sum(c => c.CellSize);
            Assert.IsTrue(1904 <= leftSize && leftSize <= 2192, "Left size should be in range 1904-2192, was {0}", leftSize);
            Assert.IsTrue(1904 <= rightSize && rightSize <= 2192, "Right size should be in range 1904-2192, was {0}", rightSize);
        }

        [TestMethod]
        public void NextPointers()
        {
            var originalNext = _leftNode.NextLeaf;
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            Assert.AreEqual(_rightNode.PageNumber, _leftNode.NextLeaf, "Next of left");
            Assert.AreEqual(originalNext, _rightNode.NextLeaf, "Next of right");
            Assert.IsTrue(_leftNode.IsDirty, "Dirty left");
            Assert.IsTrue(_rightNode.IsDirty, "Dirty right");
        }

        [TestMethod]
        public void ReturnedKeyIsSameAsFirstKeyInRightLeaf()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            Assert.AreEqual(_rightNode.GetCell(0).Key, key);
        }

        [TestMethod]
        public void SumOfCellsIsEqualToOnePlusOriginalCellsCount()
        {
            var originalCount = _leftNode.CellsCount;
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            Assert.AreEqual(originalCount + 1, _leftNode.CellsCount + _rightNode.CellsCount);
        }

        [TestMethod]
        public void CellsAreSortedInLeftLeaf()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            AssertSorted(_leftNode);
        }

        [TestMethod]
        public void CellsAreSortedInRightLeaf()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            AssertSorted(_rightNode);
        }

        private void AssertSorted(IdxLeaf leaf)
        {
            var previousKey = leaf.GetCell(0).Key;
            for (int i = 1; i < leaf.CellsCount; i++)
            {
                var currentCell = leaf.GetCell(i);
                var currentKey = currentCell.Key;
                Assert.AreEqual(i, currentCell.Ordinal, "Bad ordinal value of cell {0}", i);
                Assert.IsTrue(previousKey < currentKey, "Previous key {0} of cell {2} should be less than current {1}", previousKey, currentKey, i);
                previousKey = currentKey;
            }
        }

        [TestMethod]
        public void LastKeyOfLeftLeafIsLessThanFirstKeyInRightLeaf()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            var lastOfLeft = _leftNode.GetCell(_leftNode.CellsCount - 1).Key;
            var firstOfRight = _rightNode.GetCell(0).Key;
            Assert.IsTrue(lastOfLeft < firstOfRight);
        }

        [TestMethod]
        public void NoneOfThemIsSmallNorFull()
        {
            IdxKey key = _leftNode.Split(_rightNode, _addedCell);
            Assert.IsFalse(_leftNode.IsSmall, "Left small");
            Assert.IsFalse(_leftNode.IsFull, "Left full");
            Assert.IsFalse(_rightNode.IsSmall, "Right small");
            Assert.IsFalse(_rightNode.IsFull, "Right full");
        }
    }

    [TestClass]
    public class LeafSplitTest5 : LeafSplitTestBase
    {
        public LeafSplitTest5()
            : base(5)
        {
        }
    }

    [TestClass]
    public class LeafSplitTest200 : LeafSplitTestBase
    {
        public LeafSplitTest200()
            : base(200)
        {
        }
    }

    [TestClass]
    public class LeafSplitTestMiddle
    {
        [TestMethod]
        public void ExistsKeyThatCanBeAddedAndBeReturned()
        {
            bool found = false;
            for (int position = 78; position < 90 && !found; position++)
            {
                var leftNode = LeafSplitTestBase.CreateForSplit();
                leftNode.PageNumber = 1222;
                var rightNode = new IdxLeaf(null);
                rightNode.PageNumber = 2344;
                var addedCell = LeafSplitTestBase.CreateCell(10 * position + 5, 64);
                var key = leftNode.Split(rightNode, addedCell);
                if (key == addedCell.Key)
                    found = true;
            }
            Assert.IsTrue(found);
        }
    }
}
