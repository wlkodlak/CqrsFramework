using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class TreeInsertTest
    {
        [TestMethod]
        public void InsertShortToNewTree()
        {
            var builder = new TestTreeBuilder();
            builder.PageSize = 1024;
            builder.MinKeySize = 56;
            builder.SetNamedValue("key55.value", builder.CreateValue(24));

            IdxLeaf newRoot = builder.CreateLeaf();
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.GetPageSize()).Returns(1024);
            mock.Setup(c => c.WriteTree(0)).Returns((IIdxNode)null).Verifiable();
            mock.Setup(c => c.CreateLeaf(0)).Returns(newRoot).Verifiable();
            mock.Setup(c => c.SetTreeRoot(0, newRoot)).Verifiable();
            mock.Setup(c => c.CommitWrite(0)).Verifiable();

            var tree = new IdxTree(mock.Object, 0);
            tree.Insert(builder.BuildKey(55), builder.GetNamedValue("key55.value"));
            mock.Verify();

            Assert.AreEqual(1, newRoot.CellsCount, "Cells count");
            Assert.AreEqual(builder.BuildKey(55), newRoot.GetCell(0).Key, "Key");
            CollectionAssert.AreEqual(builder.GetNamedValue("key55.value"), newRoot.GetCell(0).ValueBytes, "Value");
        }

        [TestMethod]
        public void InsertLongToRootLeaf()
        {
            var builder = new TestTreeBuilder();
            builder.PageSize = 1024;
            builder.MinKeySize = 56;
            builder.SetNamedValue("key55.value", builder.CreateValue(1824));
            var root = builder.Leaf(2);
            root.AddContents(new int[] { 54, 56, 57, 58 }.SelectMany(i => new object[] { i, builder.CreateValue(64) }).ToArray());
            builder.Build();

            var overflow1 = builder.CreateOverflow();
            var overflow2 = builder.CreateOverflow();

            IdxLeaf leaf = (IdxLeaf)builder.GetNode(2);
            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            mock.Setup(c => c.GetPageSize()).Returns(1024);
            mock.Setup(c => c.WriteTree(0)).Returns(leaf).Verifiable();
            mock.SetupSequence(c => c.CreateOverflow(0)).Returns(overflow1).Returns(overflow2);
            mock.Setup(c => c.CommitWrite(0)).Verifiable();

            var tree = new IdxTree(mock.Object, 0);
            tree.Insert(builder.BuildKey(55), builder.GetNamedValue("key55.value"));
            mock.Verify();
            mock.Verify(c => c.CreateOverflow(0), Times.Exactly(2));

            Assert.AreEqual(5, leaf.CellsCount, "Cells count");
            var addedCell = leaf.GetCell(1);
            Assert.AreEqual(builder.BuildKey(55), addedCell.Key, "Key");
            AssertLongValue(builder.GetNamedValue("key55.value"), "Value", addedCell, 1024, overflow1, overflow2);
        }

        private void AssertLongValue(byte[] expected, string message, IdxCell cell, int pageSize, params IdxOverflow[] overflows)
        {
            Assert.AreEqual(overflows.Length, cell.OverflowLength, "{0}: expected overflow count", message);
            var buffer = new byte[cell.OverflowLength * IdxOverflow.Capacity(pageSize) + cell.ValueLength];
            Array.Copy(cell.ValueBytes, buffer, cell.ValueLength);
            if (overflows.Length > 0)
            {
                Assert.AreEqual(cell.OverflowPage, overflows[0].PageNumber, "Cell overflow page");
                for (int i = 1; i < overflows.Length; i++)
                    Assert.AreEqual(overflows[i].PageNumber, overflows[i - 1].Next, "Overflow {0} next page", i - 1);
                Assert.AreEqual(0, overflows[overflows.Length - 1].Next, "Last overflow next page");
                var offset = cell.ValueLength;
                for (int i = 0; i < overflows.Length; i++)
                    offset += overflows[i].ReadData(buffer, offset);
                Array.Resize(ref buffer, offset);
            }
            CollectionAssert.AreEqual(expected, buffer, "{0}: bytes", message);   
        }

        [TestMethod]
        public void InsertWithoutSplit()
        {
            var builder = new TestTreeBuilder();
            builder.PageSize = 1024;
            builder.MinKeySize = 56;
            builder.SetNamedValue("key55.value", builder.CreateValue(24));
            var root = builder.Interior(2);
            var lvl1 = builder.Interior(3);
            var lvl2 = builder.Leaf(4);
            root.AddContents(30, 50, lvl1, 80, 100);
            lvl1.AddContents(52, 54, lvl2, 70, 72, 74, 76, 78);
            lvl2.AddContents(new int[] { 54, 56, 57, 58 }.SelectMany(i => new object[] { i, builder.CreateValue(64) }).ToArray());
            builder.Build();

            var leaf = (IdxLeaf)builder.GetNode(4);

            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.GetPageSize()).Returns(1024);
            mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
            mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
            mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
            mock.Setup(c => c.CommitWrite(0)).Verifiable();

            var tree = new IdxTree(mock.Object, 0);
            tree.Insert(builder.BuildKey(55), builder.GetNamedValue("key55.value"));
            mock.Verify();

            Assert.AreEqual(5, leaf.CellsCount, "Cells count");
            var addedCell = leaf.GetCell(1);
            Assert.AreEqual(builder.BuildKey(55), addedCell.Key, "Key");
            AssertLongValue(builder.GetNamedValue("key55.value"), "Value", addedCell, 1024);

            foreach (var page in new int[] { 2, 3 })
                Assert.IsFalse(builder.GetNode(page).IsDirty, "Page {0} dirty", page);
        }

        [TestMethod]
        public void InsertWithRootLeafSplit()
        {
            var builder = new TestTreeBuilder();
            builder.PageSize = 1024;
            builder.MinKeySize = 56;
            builder.SetNamedValue("key55.value", builder.CreateValue(24));
            var root = builder.Leaf(2);
            root.AddContents(new int[] { 12, 38, 40, 54, 56, 57, 58 }.SelectMany(i => new object[] { i, builder.CreateValue(64) }).ToArray());
            builder.Build();
            
            var leaf1 = (IdxLeaf)builder.GetNode(2);
            var leaf2 = builder.CreateLeaf();
            var newRoot = builder.CreateInterior();

            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.GetPageSize()).Returns(1024);
            mock.Setup(c => c.WriteTree(0)).Returns(leaf1).Verifiable();
            mock.Setup(c => c.CreateLeaf(0)).Returns(leaf2).Verifiable();
            mock.Setup(c => c.CreateInterior(0)).Returns(newRoot).Verifiable();
            mock.Setup(c => c.SetTreeRoot(0, newRoot)).Verifiable();
            mock.Setup(c => c.CommitWrite(0)).Verifiable();

            var tree = new IdxTree(mock.Object, 0);
            tree.Insert(builder.BuildKey(55), builder.GetNamedValue("key55.value"));
            mock.Verify();

            Assert.AreEqual(leaf1.PageNumber, newRoot.LeftmostPage, "Root.Leftmost");
            Assert.AreEqual(leaf2.PageNumber, newRoot.GetCell(0).ChildPage, "Root[0].ChildPage");
            Assert.AreEqual(8, leaf1.CellsCount + leaf2.CellsCount, "Split leaves total cells count");
            Assert.IsTrue(leaf1.GetCell(leaf1.CellsCount - 1).Key < newRoot.GetCell(0).Key, "Left subtree keys < root key");
            Assert.IsTrue(leaf2.GetCell(0).Key >= newRoot.GetCell(0).Key, "Right subtree keys >= root key");
        }
    }
}
