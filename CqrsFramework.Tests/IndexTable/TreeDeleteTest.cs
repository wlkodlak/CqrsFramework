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
        private void AssertNodeKeys(TestTreeBuilder builder, IIdxNode node, string message, params int[] keyBases)
        {
            Assert.AreEqual(keyBases.Length, node.CellsCount, "{0} cell count", message);
            var keys = keyBases.Select(i => builder.BuildKey(i)).ToArray();
            for (int i = 0; i < node.CellsCount; i++)
                Assert.AreEqual(keys[i], node.GetCell(i).Key, "{0} cell {1} key", message, i);
        }

        [TestMethod]
        public void DeleteWithoutMerge()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
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
            mock.Verify();

            Assert.AreEqual(4, leafPage.CellsCount);
            Assert.IsFalse(Enumerable.Range(0, leafPage.CellsCount).Any(i => builder.BuildKey(113) == leafPage.GetCell(i).Key), "Deleted key 113");
        }

        [TestMethod]
        public void DeleteLongValue()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Leaf(2);
                root.AddContents(50, builder.CreateValue(56), builder.LongCell(88, builder.CreateValue(2547), 3, 4, 5));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 3)).Returns(builder.GetOverflow(3)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 4)).Returns(builder.GetOverflow(4)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 5)).Returns(builder.GetOverflow(5)).Verifiable();
                mock.Setup(c => c.Delete(0, 3)).Verifiable();
                mock.Setup(c => c.Delete(0, 4)).Verifiable();
                mock.Setup(c => c.Delete(0, 5)).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Delete(builder.BuildKey(88));
            mock.Verify();
        }

        [TestMethod]
        public void RedistributeLeafWithRight()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Interior(2);
                var lvl1 = builder.Interior(3);
                var lvl2b = builder.Leaf(5);
                var lvl2c = builder.Leaf(6);
                root.AddContents(100, 200, lvl1, 300, 400);
                lvl1.AddContents(220, 240, lvl2b, 260, lvl2c, 280, 290);
                lvl2b.AddContents(
                    new int[] { 240, 245 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                lvl2c.AddContents(
                    new int[] { 260, 262, 265 }
                    .SelectMany(i => new object[] { i, builder.CreateValue(64) })
                    .ToArray());
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(4)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(4, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(4, 5)).Returns(builder.GetNode(5)).Verifiable();
                mock.Setup(c => c.GetNode(4, 6)).Returns(builder.GetNode(6)).Verifiable();
                mock.Setup(c => c.CommitWrite(4)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 4);
            tree.Delete(builder.BuildKey(240));
            mock.Verify();

            {
                foreach (var page in new int[] { 2, 4 })
                    Assert.IsFalse(builder.GetNode(page).IsDirty, "Page {0} dirty", page);
                var leftNode = (IdxLeaf)builder.GetNode(5);
                var rightNode = (IdxLeaf)builder.GetNode(6);

                AssertNodeKeys(builder, leftNode, "Left", 245, 260);
                AssertNodeKeys(builder, rightNode, "Right", 262, 265);

                var parentCell = builder.GetNode(3).GetCell(2);
                Assert.AreEqual(builder.BuildKey(262), parentCell.Key, "Parent key");
            }
        }

        [TestMethod]
        public void MergeLeaves()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Interior(2);
                var lvl1 = builder.Interior(3);
                var leaf1 = builder.Leaf(4);
                var leaf2 = builder.Leaf(5);
                root.AddContents(100, 200, 300, lvl1, 400);
                lvl1.AddContents(310, 330, 340, 360, leaf1, 390, leaf2);
                leaf1.AddContents(360, builder.CreateValue(64), 363, builder.CreateValue(64));
                leaf2.AddContents(390, builder.CreateValue(64), 395, builder.CreateValue(64));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
                mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
                mock.Setup(c => c.Delete(0, 5)).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Delete(builder.BuildKey(390));
            mock.Verify();

            {
                Assert.IsFalse(builder.GetNode(2).IsDirty, "Root dirty");
                var lvl1 = (IdxInterior)builder.GetNode(3);
                var leaf = (IdxLeaf)builder.GetNode(4);
                Assert.AreEqual(4, lvl1.CellsCount, "Level 1 cells count");
                Assert.AreEqual(builder.BuildKey(360), lvl1.GetCell(3).Key, "Level 1 Cell 3 key");
                Assert.AreEqual(leaf.PageNumber, lvl1.GetCell(3).ChildPage, "Level 1 Cell 3 page");
                Assert.AreEqual(3, leaf.CellsCount, "Leaf cells count");
                AssertNodeKeys(builder, leaf, "Leaf", 360, 363, 395);
            }
        }

        [TestMethod]
        public void RedistributeInterior()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Interior(2);
                var int1 = builder.Interior(3);
                var int2 = builder.Interior(4);
                var lf1 = builder.Leaf(5);
                var lf2 = builder.Leaf(6);
                var lf3 = builder.Leaf(7);
                var lf4 = builder.Leaf(8);
                root.AddContents(int1, 100, int2, 200, 300);
                int1.AddContents(lf1, 20, lf2, 40, 60, 80);
                int2.AddContents(lf3, 120, lf4, 135, 150, 175, 190);
                lf1.AddContents(5, builder.CreateValue(64), 8, builder.CreateValue(64));
                lf2.AddContents(20, builder.CreateValue(64), 31, builder.CreateValue(64));
                lf3.AddContents(105, builder.CreateValue(64), 108, builder.CreateValue(64));
                lf4.AddContents(120, builder.CreateValue(64), 128, builder.CreateValue(64));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
                mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
                mock.Setup(c => c.GetNode(0, 6)).Returns(builder.GetNode(6)).Verifiable();
                mock.Setup(c => c.Delete(0, 6)).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Delete(builder.BuildKey(8));
            mock.Verify();

            {
                var root = (IdxInterior)builder.GetNode(2);
                var int1 = (IdxInterior)builder.GetNode(3);
                var int2 = (IdxInterior)builder.GetNode(4);
                var lf1 = (IdxLeaf)builder.GetNode(5);
                var lf3 = (IdxLeaf)builder.GetNode(7);
                var lf4 = (IdxLeaf)builder.GetNode(8);

                AssertNodeKeys(builder, root, "Root", 120, 200, 300);
                AssertNodeKeys(builder, int1, "Int1", 40, 60, 80, 100);
                AssertNodeKeys(builder, int2, "Int2", 135, 150, 175, 190);
                AssertNodeKeys(builder, lf1, "Leaf1", 5, 20, 31);
                Assert.AreEqual(lf1.PageNumber, int1.LeftmostPage, "Int1 leftmost page");
                Assert.AreEqual(lf3.PageNumber, int1.GetCell(3).ChildPage, "Int1 last cell page");
                Assert.AreEqual(lf4.PageNumber, int2.LeftmostPage, "Int2 leftmost page");
            }
        }

        [TestMethod]
        public void DropRoot()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Interior(2);
                var int1 = builder.Interior(3);
                var int2 = builder.Interior(4);
                var lf1 = builder.Leaf(5);
                var lf2 = builder.Leaf(6);
                var lf3 = builder.Leaf(7);
                root.AddContents(int1, 100, int2);
                int1.AddContents(30, 50, 80, lf1, 90, lf2);
                int2.AddContents(lf3, 120, 140, 150, 170);
                lf1.AddContents(80, builder.CreateValue(64), 87, builder.CreateValue(64));
                lf2.AddContents(90, builder.CreateValue(64), 94, builder.CreateValue(64));
                lf3.AddContents(110, builder.CreateValue(64), 118, builder.CreateValue(64));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
                mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
                mock.Setup(c => c.GetNode(0, 6)).Returns(builder.GetNode(6)).Verifiable();
                mock.Setup(c => c.Delete(0, 6)).Verifiable();
                mock.Setup(c => c.Delete(0, 4)).Verifiable();
                mock.Setup(c => c.Delete(0, 2)).Verifiable();
                mock.Setup(c => c.SetTreeRoot(0, builder.GetNode(3))).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Delete(builder.BuildKey(94));
            mock.Verify();

            {
                var int1 = (IdxInterior)builder.GetNode(3);
                var lf1 = (IdxLeaf)builder.GetNode(5);
                var lf3 = (IdxLeaf)builder.GetNode(7);

                AssertNodeKeys(builder, int1, "Root", 30, 50, 80, 100, 120, 140, 150, 170);
                AssertNodeKeys(builder, lf1, "Leaf1", 80, 87, 90);
                Assert.AreEqual(lf1.PageNumber, int1.GetCell(2).ChildPage, "Root cell 2 page");
                Assert.AreEqual(lf3.PageNumber, int1.GetCell(3).ChildPage, "Root cell 3 page");
            }

        }

        [TestMethod]
        public void Purge()
        {
            var builder = new TestTreeBuilder(1024, 56);
            {
                var root = builder.Interior(2);
                var lf1 = builder.Leaf(3);
                var lf2 = builder.Leaf(4);
                root.AddContents(lf1, 100, lf2);
                lf1.AddContents(
                    builder.LongCell(10, builder.CreateValue(820), 5),
                    builder.LongCell(20, builder.CreateValue(1520), 6, 7),
                    builder.LongCell(30, builder.CreateValue(750), 8),
                    builder.LongCell(40, builder.CreateValue(50)),
                    builder.LongCell(50, builder.CreateValue(60)),
                    builder.LongCell(60, builder.CreateValue(50)),
                    builder.LongCell(70, builder.CreateValue(55)),
                    builder.LongCell(80, builder.CreateValue(44)));
                lf2.AddContents(
                    builder.LongCell(100, builder.CreateValue(33)),
                    builder.LongCell(110, builder.CreateValue(62)),
                    builder.LongCell(130, builder.CreateValue(57)),
                    builder.LongCell(140, builder.CreateValue(550), 9),
                    builder.LongCell(150, builder.CreateValue(330), 10),
                    builder.LongCell(160, builder.CreateValue(60)),
                    builder.LongCell(170, builder.CreateValue(1424), 11, 12),
                    builder.LongCell(180, builder.CreateValue(33)));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 5)).Returns(builder.GetOverflow(5)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 6)).Returns(builder.GetOverflow(6)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 7)).Returns(builder.GetOverflow(7)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 8)).Returns(builder.GetOverflow(8)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 9)).Returns(builder.GetOverflow(9)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 10)).Returns(builder.GetOverflow(10)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 11)).Returns(builder.GetOverflow(11)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 12)).Returns(builder.GetOverflow(12)).Verifiable();
                mock.Setup(c => c.Delete(0, 2)).Verifiable();
                mock.Setup(c => c.Delete(0, 4)).Verifiable();
                mock.Setup(c => c.Delete(0, 5)).Verifiable();
                mock.Setup(c => c.Delete(0, 6)).Verifiable();
                mock.Setup(c => c.Delete(0, 7)).Verifiable();
                mock.Setup(c => c.Delete(0, 8)).Verifiable();
                mock.Setup(c => c.Delete(0, 9)).Verifiable();
                mock.Setup(c => c.Delete(0, 10)).Verifiable();
                mock.Setup(c => c.Delete(0, 11)).Verifiable();
                mock.Setup(c => c.Delete(0, 12)).Verifiable();
                mock.Setup(c => c.SetTreeRoot(0, builder.GetNode(3))).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Purge();
            mock.Verify();

            {
                var lf = (IdxLeaf)builder.GetNode(3);
                Assert.AreEqual(0, lf.CellsCount);
            }            
        }
    }
}
