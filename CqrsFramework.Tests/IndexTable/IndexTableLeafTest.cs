using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableLeafTest
    {
        [TestMethod]
        public void CreateEmpty()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            Assert.AreEqual(0, leaf.Next);
            Assert.AreEqual(0, leaf.CellsCount);
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        [Ignore]
        public void CreateCellWithShortData()
        {
            IdxLeaf leaf = new IdxLeaf(null);
        }
    }
}
