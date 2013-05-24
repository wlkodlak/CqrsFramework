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
            var leaf = new IdxLeaf(null, 4096);
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
            var root = new IdxLeaf(null, 4096);
            root.PageNumber = 47;
            root.NextLeaf = 0;
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(2), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(4), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(9), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(17), ContainerTestUtilities.CreateBytes(random, 84), 4096));
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
            var root = new IdxLeaf(null, 4096);
            root.PageNumber = 47;
            root.NextLeaf = 0;
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(2), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(5), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(7), ContainerTestUtilities.CreateBytes(random, 84), 4096));
            root.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(9), ContainerTestUtilities.CreateBytes(random, 84), 4096));
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
    }
}
