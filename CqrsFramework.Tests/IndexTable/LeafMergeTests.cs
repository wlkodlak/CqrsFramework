using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    public abstract class LeafMergeTestBase
    {
        private IdxLeaf _leftNode;
        private IdxLeaf _rightNode;

        protected abstract IdxLeaf CreateLeftNode();
        protected abstract IdxLeaf CreateRightNode();

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateLeftNode();
            _rightNode = CreateRightNode();
        }

        [TestMethod]
        public void OriginalsAreMergeable()
        {
            Assert.IsTrue(_leftNode.IsSmall || _rightNode.IsSmall);
            var splitKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).All(c => c.Key < splitKey), "Left should be all smaller");
            Assert.IsTrue(Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).All(c => c.Key >= splitKey), "Left should be all greater");
            if (!_leftNode.IsSmall)
            {
                while (_rightNode.IsSmall && _leftNode.CellsCount > 0)
                {
                    var pos = _leftNode.CellsCount - 1;
                    var cell = _leftNode.GetCell(pos);
                    _leftNode.RemoveCell(pos);
                    _rightNode.AddCell(cell);
                }
                Assert.IsTrue(_leftNode.IsSmall, "Left becames small after changing right to nonsmall");
            }
            if (!_rightNode.IsSmall)
            {
                while (_leftNode.IsSmall && _rightNode.CellsCount > 0)
                {
                    var cell = _rightNode.GetCell(0);
                    _rightNode.RemoveCell(0);
                    _leftNode.AddCell(cell);
                }
                Assert.IsTrue(_rightNode.IsSmall, "Right becames small after changing left to nonsmall");
            }
        }

        [TestMethod]
        public void CellCountIsEqualToSumOfOriginalCellCounts()
        {
            var originalCount = _leftNode.CellsCount + _rightNode.CellsCount;
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.AreEqual(originalCount, _leftNode.CellsCount);
        }

        [TestMethod]
        public void ResultingLeafHasCorrectOrdinalValues()
        {
            IdxKey key = _leftNode.Merge(_rightNode);
            for (int i = 0; i < _leftNode.CellsCount; i++)
                Assert.AreEqual(i, _leftNode.GetCell(i).Ordinal, "Ordinal for cell {0}", i);
        }

        [TestMethod]
        public void LeafIsNotSmallNorFull()
        {
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.IsFalse(_leftNode.IsSmall, "Small");
            Assert.IsFalse(_leftNode.IsFull, "Full");
        }

        [TestMethod]
        public void ReturnedKeyIsNull()
        {
            var originalKey = _rightNode.GetCell(0).Key;
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.IsNull(key);
        }

        [TestMethod]
        public void NextPointerAndDirty()
        {
            var originalNext = _rightNode.NextLeaf;
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.AreEqual(originalNext, _leftNode.NextLeaf, "Next pointer");
            Assert.IsTrue(_leftNode.IsDirty, "Dirty");
        }

        public static void AddNodeCell(IdxLeaf node, int keyBase, int cellLength)
        {
            var key = IdxKey.FromInteger(keyBase);
            var cellData = new byte[cellLength - 12];
            var cell = IdxCell.CreateLeafCell(key, cellData, 4096);
            node.AddCell(cell);
        }
    }

    [TestClass]
    public class LeafMergeSmallTrees : LeafMergeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 1; i <= 8; i++)
                AddNodeCell(node, i * 10, 128);
            node.NextLeaf = 5647;
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 11; i <= 17; i++)
                AddNodeCell(node, i * 10, 128);
            node.NextLeaf = 1234;
            return node;
        }
    }

    [TestClass]
    public class LeafMergeBorderLine : LeafMergeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 1; i <= 7; i++)
                AddNodeCell(node, i * 10, 128);
            node.NextLeaf = 5647;
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 1; i <= 69; i++)
                AddNodeCell(node, 100 + i * 10, 16);
            node.NextLeaf = 234;
            return node;
        }
    }

    [TestClass]
    public class LeafMergeBigLastCell : LeafMergeTestBase
    {
        protected override IdxLeaf CreateLeftNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 1; i <= 4; i++)
                AddNodeCell(node, i * 10, 16);
            for (int i = 9; i <= 16; i++)
                AddNodeCell(node, i * 10, 128);
            node.NextLeaf = 5647;
            return node;
        }

        protected override IdxLeaf CreateRightNode()
        {
            IdxLeaf node = new IdxLeaf(null, 4096);
            for (int i = 101; i <= 160; i++)
                AddNodeCell(node, i * 10, 16);
            node.NextLeaf = 633;
            return node;
        }
    }
}
