using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    public abstract class InteriorMergeTestBase
    {
        private IdxInterior _leftNode, _rightNode;
        private IdxCell _parentCell;

        protected abstract IdxInterior CreateLeftNode();
        protected abstract IdxInterior CreateRightNode();
        protected abstract IdxCell CreateMiddleCell();

        public static IdxCell CreateCell(int keyBase, int page, int cellSize)
        {
            var bytes = new byte[cellSize - 8];
            bytes[0] = (byte)((keyBase >> 24) & 0xff);
            bytes[1] = (byte)((keyBase >> 16) & 0xff);
            bytes[2] = (byte)((keyBase >> 8) & 0xff);
            bytes[3] = (byte)(keyBase & 0xff);
            return IdxCell.CreateInteriorCell(IdxKey.FromBytes(bytes), page);
        }

        [TestInitialize]
        public void Initialize()
        {
            _leftNode = CreateLeftNode();
            _rightNode = CreateRightNode();
            _parentCell = CreateMiddleCell();
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
                var middle = _parentCell;
                while (_rightNode.IsSmall && _leftNode.CellsCount > 0)
                {
                    var pos = _leftNode.CellsCount - 1;
                    var cell = _leftNode.GetCell(pos);
                    _leftNode.RemoveCell(pos);
                    _rightNode.AddCell(middle);
                    middle = cell;
                }
                Assert.IsTrue(_leftNode.IsSmall, "Left becames small after changing right to nonsmall");
            }
            if (!_rightNode.IsSmall)
            {
                var middle = _parentCell;
                while (_leftNode.IsSmall && _rightNode.CellsCount > 0)
                {
                    var cell = _rightNode.GetCell(0);
                    _rightNode.RemoveCell(0);
                    _leftNode.AddCell(middle);
                    middle = cell;
                }
                Assert.IsTrue(_rightNode.IsSmall, "Right becames small after changing left to nonsmall");
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

    [TestClass]
    public class InteriorMergeTestSameSize : InteriorMergeTestBase
    {
        private void AddCells(IdxInterior node, params int[] keyBases)
        {
            foreach (int keyBase in keyBases)
                node.AddCell(CreateCell(keyBase, keyBase * 17, 64));
        }

        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 29;
            node.LeftmostPage = 8;
            AddCells(node, 5, 7, 8, 9, 12, 15, 18, 22, 25, 29, 33, 37, 38, 40, 44);
            return node;
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 932;
            node.LeftmostPage = 991;
            AddCells(node, 52, 55, 59, 60, 62, 64, 68, 70, 72, 73, 74, 75, 80, 82, 86, 88);
            return node;
        }

        protected override IdxCell CreateMiddleCell()
        {
            return CreateCell(49, 932, 64);
        }
    }

    [TestClass]
    public class InteriorMergeTestBorderLine : InteriorMergeTestBase
    {
        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1001;
            node.LeftmostPage = 2001;
            for (int i = 0; i < 8; i++)
                node.AddCell(CreateCell(i * 10, 3 + i * 17, 128));
            for (int i = 0; i < 4; i++)
                node.AddCell(CreateCell(100 + i * 10, i * 17, 16));
            return node;
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1002;
            node.LeftmostPage = 2004;
            for (int i = 0; i < 56; i++)
                node.AddCell(CreateCell(200 + i * 10, 5 + i * 17, 16));
            return node;
        }

        protected override IdxCell CreateMiddleCell()
        {
            return CreateCell(190, 1002, 32);
        }
    }

    [TestClass]
    public class InteriorMergeTestBigCell : InteriorMergeTestBase
    {
        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1001;
            node.LeftmostPage = 2001;
            for (int i = 0; i < 6; i++)
                node.AddCell(CreateCell(i * 10, i * 17, 16));
            for (int i = 0; i < 8; i++)
                node.AddCell(CreateCell(100 + i * 10, 3 + i * 17, 128));
            return node;
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1002;
            node.LeftmostPage = 2004;
            for (int i = 0; i < 60; i++)
                node.AddCell(CreateCell(200 + i * 10, 5 + i * 17, 16));
            return node;
        }

        protected override IdxCell CreateMiddleCell()
        {
            return CreateCell(190, 1002, 128);
        }
    }

    [TestClass]
    public class InteriorMergeTestBigMiddleCell : InteriorMergeTestBase
    {
        protected override IdxInterior CreateLeftNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1001;
            node.LeftmostPage = 1;
            for (int i = 0; i < 62; i++)
                node.AddCell(CreateCell(i, 2 + i, 16));
            return node;
        }

        protected override IdxInterior CreateRightNode()
        {
            var node = new IdxInterior(null);
            node.PageNumber = 1002;
            node.LeftmostPage = 64;
            node.AddCell(CreateCell(63, 65, 128));
            for (int i = 0; i < 62; i++)
                node.AddCell(CreateCell(64 + i, 66 + i, 16));
            return node;
        }

        protected override IdxCell CreateMiddleCell()
        {
            return CreateCell(62, 1002, 16);
        }
    }
}
