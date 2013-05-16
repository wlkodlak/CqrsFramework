using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableLeafMergeTests
    {
        private IdxLeaf _leftNode;
        private IdxLeaf _rightNode;

        private IdxLeaf CreateNode(bool left)
        {
            int keyPlus = left ? 0 : 100;
            IdxLeaf node = new IdxLeaf(null);
            for (int i = 1; i <= 7; i++)
                node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i * 10 + keyPlus), new byte[116]));
            node.NextLeaf = left ? 5647 : 1234;
            return node;
        }

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateNode(true);
            _rightNode = CreateNode(false);
        }

        [TestMethod]
        public void OriginalsAreMergeable()
        {
            Assert.IsTrue(_leftNode.IsSmall);
            Assert.IsTrue(_rightNode.IsSmall);
            var splitKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(Enumerable.Range(0, _leftNode.CellsCount).Select(i => _leftNode.GetCell(i)).All(c => c.Key < splitKey), "Left should be all smaller");
            Assert.IsTrue(Enumerable.Range(0, _rightNode.CellsCount).Select(i => _rightNode.GetCell(i)).All(c => c.Key >= splitKey), "Left should be all greater");
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
        public void ReturnedKeyIsEqualToOriginalFirstRightKey()
        {
            var originalKey = _rightNode.GetCell(0).Key;
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.AreEqual(originalKey, key);
        }

        [TestMethod]
        public void NextPointerAndDirty()
        {
            var originalNext = _rightNode.NextLeaf;
            IdxKey key = _leftNode.Merge(_rightNode);
            Assert.AreEqual(originalNext, _leftNode.NextLeaf, "Next pointer");
            Assert.IsTrue(_leftNode.IsDirty, "Dirty");
        }
    }
}
