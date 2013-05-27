using System;
using System.Linq;
using System.Collections.Generic;
using Moq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class TreeReadTest
    {
        [TestMethod]
        public void NonexistentTree()
        {
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(3)).Returns((IIdxNode)null).Verifiable();

            IdxTree tree = new IdxTree(mock.Object, 3);
            var selected = tree.Select(IdxKey.FromInteger(5), IdxKey.FromInteger(8)).ToList();
            Assert.AreEqual(0, selected.Count, "Empty result");
            mock.Verify();
        }

        [TestMethod]
        public void EmptyTree()
        {
            var leaf = new IdxLeaf(null, 512);
            leaf.PageNumber = 47;
            leaf.Save();

            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(3)).Returns(leaf).Verifiable();
            mock.Setup(c => c.UnlockRead(3)).Verifiable();

            IdxTree tree = new IdxTree(mock.Object, 3);
            var selected = tree.Select(IdxKey.FromInteger(5), IdxKey.FromInteger(8)).ToList();
            Assert.AreEqual(0, selected.Count, "Empty result");
            mock.Verify();
        }

        [TestMethod]
        public void SingleLeafWithShortValuesButNoResult()
        {
            var random = new Random(84324);
            var root = new IdxLeaf(null, 512);
            root.PageNumber = 47;
            root.NextLeaf = 0;
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(2), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(4), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(9), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(17), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.Save();

            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(3)).Returns(root);
            mock.Setup(c => c.UnlockRead(3)).Verifiable();

            IdxTree tree = new IdxTree(mock.Object, 3);
            var selected = tree.Select(IdxKey.FromInteger(5), IdxKey.FromInteger(8)).ToList();
            Assert.AreEqual(0, selected.Count, "Empty result");
            mock.Verify();
        }

        [TestMethod]
        public void SingleLeafWithShortValuesWithResults()
        {
            var random = new Random(84324);
            var root = new IdxLeaf(null, 512);
            root.PageNumber = 47;
            root.NextLeaf = 0;
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(2), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(5), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(7), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(9), ContainerTestUtilities.CreateBytes(random, 84), 512));
            root.Save();

            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(3)).Returns(root);
            mock.Setup(c => c.UnlockRead(3)).Verifiable();

            IdxTree tree = new IdxTree(mock.Object, 3);
            var selected = tree.Select(IdxKey.FromInteger(5), IdxKey.FromInteger(8)).ToList();
            Assert.AreEqual(2, selected.Count, "Result size");
            var expectedResult = Enumerable.Range(0, root.CellsCount).Select(i => root.GetCell(i))
                .Where(c => c.Key >= IdxKey.FromInteger(5) && c.Key <= IdxKey.FromInteger(8))
                .Select(c => new KeyValuePair<IdxKey, byte[]>(c.Key, c.ValueBytes))
                .ToList();
            CollectionAssert.AreEqual(expectedResult, selected, "Result items");
            mock.Verify();
        }

        private TestTreeBuilder CreateComplexTree()
        {
            var builder = new TestTreeBuilder(1024, 120);
            builder.SetNamedValue("key51.value", builder.CreateValue(1744));
            var root = builder.Interior(2);
            var lvl1 = builder.Interior(3);
            var lvl2a = builder.Leaf(4);
            var lvl2b = builder.Leaf(5);
            var lvl2c = builder.Leaf(6);
            var lvl2d = builder.Leaf(7);
            root.AddContents(20, 50, lvl1, 80, 100);
            lvl1.AddContents(50, lvl2a, 54, lvl2b, 60, lvl2c, 68, lvl2d, 75);
            lvl2a.AddContents(50, builder.LongCell(51, builder.GetNamedValue("key51.value"), 10, 11));
            lvl2b.AddContents(54, 56, 57, 58);
            lvl2c.AddContents(60, 62, 63);
            lvl2d.AddContents(68, 69, 70, 72, 74);
            builder.Build();
            return builder;
        }

        [TestMethod]
        public void SearchForNonexistentKey()
        {
            var builder = CreateComplexTree();
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(0)).Returns(builder.GetNode(2)).Verifiable();
            mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
            mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
            mock.Setup(c => c.UnlockRead(0));

            IdxTree tree = new IdxTree(mock.Object, 0);
            var selected = tree.Select(builder.BuildKey(55), builder.BuildKey(55));
            Assert.AreEqual(0, selected.Count());
            mock.Verify();
        }

        [TestMethod]
        public void SearchForRangeInSingleNode()
        {
            var builder = CreateComplexTree();
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(0)).Returns(builder.GetNode(2)).Verifiable();
            mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
            mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
            mock.Setup(c => c.UnlockRead(0));

            IdxTree tree = new IdxTree(mock.Object, 0);
            var selected = tree.Select(builder.BuildKey(56), builder.BuildKey(58)).ToList();
            Assert.AreEqual(3, selected.Count, "Count");
            Assert.AreEqual(builder.BuildKey(56), selected[0].Key, "Key 56");
            Assert.AreEqual(builder.BuildKey(57), selected[1].Key, "Key 57");
            Assert.AreEqual(builder.BuildKey(58), selected[2].Key, "Key 58");
            mock.Verify();
        }

        [TestMethod]
        public void SearchForRangeAcrossNodes()
        {
            var builder = CreateComplexTree();
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.ReadTree(0)).Returns(builder.GetNode(2)).Verifiable();
            mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
            mock.Setup(c => c.GetNode(0, 5)).Returns(builder.GetNode(5)).Verifiable();
            mock.Setup(c => c.GetNode(0, 6)).Returns(builder.GetNode(6)).Verifiable();
            mock.Setup(c => c.GetNode(0, 7)).Returns(builder.GetNode(7)).Verifiable();
            mock.Setup(c => c.UnlockRead(0));
            var tree = new IdxTree(mock.Object, 0);
            var selected = tree.Select(builder.BuildKey(55), builder.BuildKey(71)).ToList();
            var keyBases = new int[] { 56, 57, 58, 60, 62, 63, 68, 69, 70 };
            Assert.AreEqual(keyBases.Length, selected.Count, "Count");
            for (int i = 0; i < keyBases.Length; i++)
                Assert.AreEqual(builder.BuildKey(keyBases[i]), selected[i].Key, "Key {0}", keyBases[i]);
            mock.Verify();
        }

        [TestMethod]
        public void LoadOverflowData()
        {
            var builder = CreateComplexTree();
            var mock = new Mock<IIdxContainer>();
            mock.Setup(c => c.GetPageSize()).Returns(1024);
            mock.Setup(c => c.ReadTree(0)).Returns(builder.GetNode(2)).Verifiable();
            mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
            mock.Setup(c => c.GetNode(0, 4)).Returns(builder.GetNode(4)).Verifiable();
            mock.Setup(c => c.GetOverflow(0, 10)).Returns(builder.GetOverflow(10)).Verifiable();
            mock.Setup(c => c.GetOverflow(0, 11)).Returns(builder.GetOverflow(11)).Verifiable();
            mock.Setup(c => c.UnlockRead(0));
            var tree = new IdxTree(mock.Object, 0);
            var selected = tree.Select(builder.BuildKey(51), builder.BuildKey(51)).FirstOrDefault();
            Assert.IsNotNull(selected, "Found");
            Assert.AreEqual(builder.BuildKey(51), selected.Key, "Key");
            CollectionAssert.AreEqual(builder.GetNamedValue("key51.value"), selected.Value, "Contents");
            mock.Verify();
        }
    }
}
