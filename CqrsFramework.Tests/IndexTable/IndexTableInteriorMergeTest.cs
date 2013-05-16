using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    [Ignore]
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
            _leftNode = CreateNode(true);
            _rightNode = CreateNode(false);
        }

        [TestMethod]
        public void BothAreMergeable()
        {
            Assert.IsTrue(_leftNode.IsSmall, "Left small");
            Assert.IsTrue(_rightNode.IsSmall, "Right small");
            Assert.IsTrue(_leftNode.GetCell(_leftNode.CellsCount - 1).Key < _parentCell.Key, "Left less than parent key");
            Assert.IsTrue(_leftNode.GetCell(0).Key > _parentCell.Key, "Right greater than parent key");

        }
    }
}
