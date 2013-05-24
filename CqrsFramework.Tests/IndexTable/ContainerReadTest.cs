using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;
using Moq;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class ContainerReadTest
    {
        [TestMethod]
        public void LoadEmpty()
        {
            var file = new MemoryPagedFile();

            using (var container = new IdxContainer(file))
            {
                Assert.IsInstanceOfType(container, typeof(IIdxContainer), "Implements IIdxContainer");
            }
            Assert.IsTrue(file.Disposed, "File was not disposed");
        }

        [TestMethod]
        public void LoadFileWithHeaderAndEmptyTable()
        {
            IdxHeader header = new IdxHeader(null, 4096);
            header.FreePagesList = 1;
            header.TotalPagesCount = 4;
            header.SetTreeRoot(0, 2);
            IdxFreeList freeList = new IdxFreeList(null, 4096);
            freeList.Add(3);
            IdxLeaf tableNode = new IdxLeaf(null, 4096);

            var file = new MemoryPagedFile(4);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 4, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 3);
            file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();

            using (var container = new IdxContainer(file))
            {
            }

            Assert.IsFalse(file.ChangedSize, "Size changed");
            Assert.IsTrue(file.Disposed, "File was not disposed");
        }

        [TestMethod]
        public void ReadTableFromEmptyContainer()
        {
            var file = new MemoryPagedFile();
            using (var container = new IdxContainer(file))
            {
                IIdxNode root = container.ReadTree(0);
                Assert.IsNull(root);
            }
            Assert.IsFalse(file.ChangedSize, "Size changed");
            Assert.IsTrue(file.Disposed, "File was not disposed");
        }

        [TestMethod]
        public void ReadNonExistentTable()
        {
            var file = new MemoryPagedFile(4);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 4);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 2, 3);
            using (var container = new IdxContainer(file))
            {
                IIdxNode root = container.ReadTree(0);
                Assert.IsNull(root);
            }
            Assert.IsFalse(file.ChangedSize, "Size changed");
            Assert.IsTrue(file.Disposed, "File was not disposed");
        }

        private byte[] _longCellValue;
        private int _longCellOffset = 116;

        private MemoryPagedFile PrepareFileForReads()
        {
            _longCellValue = new byte[3022];
            new Random(4920).NextBytes(_longCellValue);

            var longCell0 = IdxCell.CreateLeafCell(IdxKey.FromInteger(14), _longCellValue, 4096);
            _longCellOffset = longCell0.ValueLength;

            var overflow0 = new IdxOverflow(null, 4096);
            overflow0.WriteData(_longCellValue, longCell0.ValueLength);
            longCell0.OverflowPage = 5;

            var longCell1 = IdxCell.CreateLeafCell(IdxKey.FromInteger(140), _longCellValue, 4096);

            var overflow1 = new IdxOverflow(null, 4096);
            overflow1.WriteData(_longCellValue, longCell1.ValueLength);
            longCell1.OverflowPage = 7;

            var file = new MemoryPagedFile(16);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 16, 0, 4, 6);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, Enumerable.Range(9, 7).ToArray());
            file.Pages[2] = NodeBuilder.Leaf(3)
                .AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(8), new byte[48], 4096))
                .AddCell(longCell0).ToBytes();
            file.Pages[3] = NodeBuilder.Leaf(0)
                .AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(180), new byte[100], 4096))
                .AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(392), new byte[100], 4096))
                .ToBytes();
            file.Pages[4] = NodeBuilder.Interior(2)
                .AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(100), 8, 4096))
                .ToBytes();
            file.Pages[5] = overflow0.Save();
            file.Pages[6] = NodeBuilder.Leaf(38)
                .AddCell(longCell1).ToBytes();
            file.Pages[7] = overflow1.Save();
            file.Pages[8] = NodeBuilder.Interior(11)
                .AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(199), 3, 4096)).ToBytes();

            return file;
        }

        [TestMethod]
        public void ReadLeafRoot()
        {
            var file = PrepareFileForReads();
            using (var container = new IdxContainer(file))
            {
                var root = container.ReadTree(2);
                Assert.IsInstanceOfType(root, typeof(IdxLeaf), "Loaded leaf");
                var leaf = root as IdxLeaf;
                Assert.AreEqual(6, leaf.PageNumber, "Page number");
                Assert.AreEqual(38, leaf.NextLeaf, "Next leaf");
                Assert.AreEqual(1, leaf.CellsCount, "Cells count");
                Assert.AreEqual(IdxKey.FromInteger(140), leaf.GetCell(0).Key, "Cell 0 key");
                container.UnlockRead(2);
            }
        }

        [TestMethod]
        public void ReadOverflow()
        {
            var file = PrepareFileForReads();
            using (var container = new IdxContainer(file))
            {
                var valueBytes = new byte[3022];
                var root = container.ReadTree(2) as IdxLeaf;
                Assert.IsNotNull(root, "IdxLeaf expected as root 2");
                Array.Copy(root.GetCell(0).ValueBytes, valueBytes, root.GetCell(0).ValueLength);
                var overflow = container.GetOverflow(2, 7);
                Assert.AreEqual(7, overflow.PageNumber, "Overflow page number");
                Assert.IsNotNull(overflow, "IdxOverflow expected as page 7");
                Assert.AreEqual(0, overflow.Next, "Overflow next");
                Assert.AreEqual(2906, overflow.LengthInPage, "Overflow length");
                overflow.ReadData(valueBytes, root.GetCell(0).ValueLength);
                CollectionAssert.AreEqual(_longCellValue, valueBytes, "Overflow value");
                container.UnlockRead(2);
            }
        }

        [TestMethod]
        public void ReadInteriorRoot()
        {
            var file = PrepareFileForReads();
            using (var container = new IdxContainer(file))
            {
                var root = container.ReadTree(1);
                Assert.AreEqual(4, root.PageNumber, "Page number");
                Assert.IsInstanceOfType(root, typeof(IdxInterior), "Root type");
                var node = root as IdxInterior;
                Assert.AreEqual(8, node.GetCell(0).ChildPage, "Cell 0 page");
                Assert.AreEqual(IdxKey.FromInteger(100), node.GetCell(0).Key, "Cell 0 key");
                container.UnlockRead(1);
            }
        }

        [TestMethod]
        public void ReadLeafNode()
        {
            var file = PrepareFileForReads();
            using (var container = new IdxContainer(file))
            {
                var root = container.ReadTree(1);
                var node = container.GetNode(1, 2);
                Assert.AreEqual(2, node.PageNumber, "Page number");
                Assert.IsInstanceOfType(node, typeof(IdxLeaf));
                var leaf = node as IdxLeaf;
                Assert.AreEqual(3, leaf.NextLeaf, "Next leaf");
                Assert.AreEqual(2, leaf.CellsCount, "Cells count");
                container.UnlockRead(1);
            }
        }

        [TestMethod]
        public void ReadInteriorNode()
        {
            var file = PrepareFileForReads();
            using (var container = new IdxContainer(file))
            {
                var root = container.ReadTree(1);
                var node = container.GetNode(1, 8);
                Assert.AreEqual(8, node.PageNumber, "Page number");
                Assert.IsInstanceOfType(node, typeof(IdxInterior));
                var interior = node as IdxInterior;
                Assert.AreEqual(11, interior.LeftmostPage, "Next leaf");
                Assert.AreEqual(1, interior.CellsCount, "Cells count");
                container.UnlockRead(1);
            }
        }
    }
}
