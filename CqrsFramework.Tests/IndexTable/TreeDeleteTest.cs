using System;
using System.Collections.Generic;
using System.Linq;
using Moq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class TreeDeleteTest
    {
        [TestMethod]
        public void DeleteWithoutMerge()
        {
            var builder = new TestTreeBuilder();
            {
                builder.PageSize = 1024;
                builder.MinKeySize = 56;
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(100, leaf, 200, 300);
                leaf.AddContents(
                    new int[] { 110, 111, 113, 118, 129 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                builder.Build();
            }

            var rootPage = (IdxInterior)builder.GetNode(2);
            var leafPage = (IdxLeaf)builder.GetNode(3);

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(rootPage).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(leafPage).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Delete(builder.BuildKey(113));

            Assert.AreEqual(4, leafPage.CellsCount);
            Assert.IsFalse(Enumerable.Range(0, leafPage.CellsCount).Any(i => builder.BuildKey(113) == leafPage.GetCell(i).Key), "Deleted key 113");
        }

        [TestMethod]
        public void RedistributeLeaf()
        {
            var builder = new TestTreeBuilder();
            {
                builder.PageSize = 1024;
                builder.MinKeySize = 54;
                var root = builder.Interior(2);
                var lvl1 = builder.Interior(3);
                var lvl2a = builder.Leaf(4);
                var lvl2b = builder.Leaf(5);
                var lvl2c = builder.Leaf(6);
                root.AddContents(100, 200, lvl1, 300, 400);
                lvl1.AddContents(220, lvl2a, 240, lvl2b, 260, lvl2c, 280, 290);
                lvl2a.AddContents(
                    new int[] { 220, 221, 222, 224, 226 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                lvl2b.AddContents(
                    new int[] { 240, 245 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                lvl2c.AddContents(
                    new int[] { 260, 265 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(4)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(4, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(4, 4)).Returns(builder.GetNode(4)).Verifiable();
                mock.Setup(c => c.GetNode(4, 5)).Returns(builder.GetNode(5)).Verifiable();
                mock.Setup(c => c.GetNode(4, 6)).Returns(builder.GetNode(6)).Verifiable();
                mock.Setup(c => c.CommitWrite(4)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 4);
            tree.Delete(builder.BuildKey(240));
            mock.Verify();

            {
                foreach (var page in new int[] { 2, 6 })
                    Assert.IsFalse(builder.GetNode(page).IsDirty, "Page {0} dirty", page);
                var leftNode = (IdxLeaf)builder.GetNode(4);
                var rightNode = (IdxLeaf)builder.GetNode(5);
                Assert.AreEqual(3, leftNode.CellsCount, "Left count");
                Assert.AreEqual(3, rightNode.CellsCount, "Right count");

                var leftKeys = new int[] { 220, 221, 222 }.Select(i =>builder.BuildKey(i)).ToArray();
                var rightKeys = new int[] { 224, 226, 245 }.Select(i =>builder.BuildKey(i)).ToArray();
                for (int i = 0; i < 3; i++)
                    Assert.AreEqual(leftKeys[i], leftNode.GetCell(i), "Left key {0}", i);
                for (int i = 0; i < 3; i++)
                    Assert.AreEqual(rightKeys[i], rightNode.GetCell(i), "Right key {0}", i);

                var parentCell = builder.GetNode(3).GetCell(1);
                Assert.AreEqual(builder.BuildKey(224), parentCell.Key, "Parent key");
            }
        }
    }
}
