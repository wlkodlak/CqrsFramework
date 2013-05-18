using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableInteriorMergeTest
    {
        private IdxInterior _leftNode, _rightNode;
        private IdxCell _parentCell;

        private IdxInterior CreateNode(bool left)
        {
            return null;
        }

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = new IdxInterior(null);
            _leftNode.PageNumber = 29;
            _leftNode.LeftmostPage = 8;
            AddCells(_leftNode, 5, 7, 8, 9, 12, 15, 18, 22, 25, 29, 33, 37, 38, 40, 44);
            _parentCell = IdxCell.CreateInteriorCell(CreateKey(49), 932);
            _rightNode = new IdxInterior(null);
            _rightNode.PageNumber = 932;
            _rightNode.LeftmostPage = 991;
            AddCells(_rightNode, 52, 55, 59, 60, 62, 64, 68, 70, 72, 73, 74, 75, 80, 82, 86, 88);
        }

        private void AddCells(IdxInterior node, params int[] keyBases)
        {
            foreach (int keyBase in keyBases)
                node.AddCell(IdxCell.CreateInteriorCell(CreateKey(keyBase), keyBase * 17));
        }

        private IdxKey CreateKey(int keyBase)
        {
            var bytes = new byte[56];
            bytes[0] = (byte)((keyBase >> 24) & 0xff);
            bytes[1] = (byte)((keyBase >> 16) & 0xff);
            bytes[2] = (byte)((keyBase >>  8) & 0xff);
            bytes[3] = (byte)(keyBase & 0xff);
            return IdxKey.FromBytes(bytes);
        }

        [TestMethod]
        public void BothAreMergeable()
        {
            Assert.IsTrue(_leftNode.IsSmall || _rightNode.IsSmall, "Either small");
            var leftMaxCellKey = _leftNode.GetCell(_leftNode.CellsCount - 1).Key;
            var parentKey = _parentCell.Key;
            var rightMinKey = _rightNode.GetCell(0).Key;
            Assert.IsTrue(leftMaxCellKey < parentKey, "Left less than parent key {0} < {1}", leftMaxCellKey, parentKey);
            Assert.IsTrue(parentKey < rightMinKey, "Right greater than parent key {0} < {1}", parentKey, rightMinKey);
            Assert.AreEqual(_parentCell.ChildPage, _rightNode.PageNumber, "Right page number");
            if (!_leftNode.IsSmall)
            {
                _leftNode.RemoveCell(0);
                Assert.IsTrue(_leftNode.IsSmall, "Left becames small after single delete");
            }
            if (!_rightNode.IsSmall)
            {
                _rightNode.RemoveCell(0);
                Assert.IsTrue(_rightNode.IsSmall, "Left becames small after single delete");
            }
        }

        [TestMethod]
        public void CellCountIsOneMoreThanOriginalSum()
        {
            var originalSum = _leftNode.CellsCount + _rightNode.CellsCount;
            _leftNode.Merge(_rightNode, _parentCell);
            Assert.AreEqual(originalSum + 1, _leftNode.CellsCount);
        }

        [TestMethod]
        public void CellsAreOrdered()
        {
            _leftNode.Merge(_rightNode, _parentCell);
            Assert.AreEqual(0, _leftNode.GetCell(0).Ordinal, "Cell 0 ordinal");
            for (int i = 1; i < _leftNode.CellsCount; i++)
            {
                var prevKey = _leftNode.GetCell(i - 1).Key;
                var currKey = _leftNode.GetCell(i).Key;
                var currOrdinal = _leftNode.GetCell(i).Ordinal;
                Assert.IsTrue(prevKey < currKey,
                    "Cell {0} key should be less than key of cell {1}; {2} < {3}",
                    i - 1, i, prevKey, currKey);
                Assert.AreEqual(i, currOrdinal, "Cell {0} ordinal", i);
            }
        }

        [TestMethod]
        public void ParentCellIncorporatedToNode()
        {
            var incorporatedIndex = _leftNode.CellsCount;
            var originalRightLeftmost = _rightNode.LeftmostPage;
            var resultKey = _leftNode.Merge(_rightNode, _parentCell);
            Assert.IsNull(resultKey, "Key should be null, because we merge the nodes into one");
            var incorporatedCell = _leftNode.GetCell(incorporatedIndex);
            Assert.AreEqual(_parentCell.Key, incorporatedCell.Key, "Incorporated key");
            Assert.AreEqual(originalRightLeftmost, incorporatedCell.ChildPage, "Incorporated page");
            Assert.IsFalse(_leftNode.IsSmall, "Small");
            Assert.IsFalse(_leftNode.IsFull, "Full");
        }
    }
}
