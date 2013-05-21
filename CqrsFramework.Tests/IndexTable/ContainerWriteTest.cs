using System;
using CqrsFramework.IndexTable;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class ContainerWriteTest
    {
        [TestMethod]
        public void WriteOnEmptyWithoutActuallyChangingAnything()
        {
            var file = new MemoryPagedFile();
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(0);
                container.CommitWrite(0);
            }
            Assert.IsTrue(file.Pages.Count >= 2, "At least two pages");
            var header = new IdxHeader(file.Pages[0]);
            Assert.AreEqual(file.Pages.Count, header.TotalPagesCount, "Header.TotalPagesCount");
            Assert.AreEqual(1, header.FreePagesList, "Freelist page");
            var freeList = new IdxFreeList(file.Pages[header.FreePagesList]);
            Assert.AreEqual(file.Pages.Count - 2, freeList.Length, "FreeList.Length");
        }

        [TestMethod]
        public void CreateEmptyLeafInNewTable()
        {
            var file = new MemoryPagedFile();
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(3);
                var leaf = container.CreateLeaf(3);
                container.SetTreeRoot(3, leaf);
                Assert.IsFalse(file.WrittenPages.Any(i => i > 1), "Pages written before commit except header and freelist");
                container.CommitWrite(3);
            }

            {
                var header = new IdxHeader(file.Pages[0]);
                var freelist = new IdxFreeList(file.Pages[header.FreePagesList]);
                var leafPage = header.GetTreeRoot(3);
                Assert.IsTrue(leafPage > 0, "Leaf page number cannot be 0");
                Assert.IsFalse(
                    Enumerable.Range(0, freelist.Length).Select(i => freelist.Alloc()).Any(p => p == leafPage),
                    "Freelist contains leaf page");
                var leaf = new IdxLeaf(file.Pages[leafPage]);
                Assert.AreEqual(0, leaf.CellsCount);
            }
        }

        [TestMethod]
        public void AllocateSeveralPages()
        {
            var file = new MemoryPagedFile(4);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 4, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 3);
            file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(1);
                for (int i = 0; i < 5; i++)
                    container.CreateLeaf(1);
                container.CommitWrite(1);
            }
            Assert.AreEqual(8, file.Pages.Count);
            var freelist = new IdxFreeList(file.Pages[1]);
            Assert.IsTrue(freelist.IsEmpty);
        }

        [TestMethod]
        public void AllocatePagesInBigFile()
        {
            var file = new MemoryPagedFile(8 * 1024);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8 * 1024, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0);
            file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(1);
                for (int i = 0; i < 1022; i++)
                    container.CreateLeaf(1);
                container.CommitWrite(1);
            }
            var header = new IdxHeader(file.Pages[0]);
            Assert.AreEqual(9 * 1024, file.Pages.Count);
            var freelist = new IdxFreeList(file.Pages[header.FreePagesList]);
            Assert.AreEqual(2, freelist.Length);
        }

        [TestMethod]
        public void CreateLeafWithLongValueInNewTable()
        {
            var savedData = new byte[8726];

            var file = new MemoryPagedFile(4);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 4);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 3, 2);
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(0);
                var cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(8547), savedData);
                int offset = cell.ValueLength;
                IdxOverflow previousOverflow = null;
                while (offset < savedData.Length)
                {
                    var overflow = container.CreateOverflow(0);
                    offset += overflow.WriteData(savedData, offset);
                    if (previousOverflow == null)
                        cell.OverflowPage = overflow.PageNumber;
                    else
                        previousOverflow.Next = overflow.PageNumber;
                    previousOverflow = overflow;
                }
                var leaf = container.CreateLeaf(0);
                leaf.AddCell(cell);
                container.SetTreeRoot(0, leaf);
                container.CommitWrite(0);
            }
            {
                var header = new IdxHeader(file.Pages[0]);
                var freelist = new IdxFreeList(file.Pages[header.FreePagesList]);
                var leaf = new IdxLeaf(file.Pages[header.GetTreeRoot(0)]);
                var cell = leaf.GetCell(0);
                var buffer = new byte[IdxOverflow.Capacity * cell.OverflowLength + cell.ValueLength];
                Array.Copy(cell.ValueBytes, buffer, cell.ValueLength);
                var offset = cell.ValueLength;
                var overflowPage = cell.OverflowPage;
                var dataLength = cell.ValueLength;
                while (overflowPage != 0)
                {
                    var overflow = new IdxOverflow(file.Pages[overflowPage]);
                    var nowRead = overflow.ReadData(buffer, offset);
                    offset += nowRead;
                    dataLength += nowRead;
                    overflowPage = overflow.Next;
                }
                CollectionAssert.AreEqual(savedData, buffer.Take(dataLength).ToArray());
            }
        }

        [TestMethod]
        public void AddLongCellToLeaf()
        {
            var random = new Random(6844234);
            var cell0Data = ContainerTestUtilities.CreateBytes(random, 47);
            var cell1Data = ContainerTestUtilities.CreateBytes(random, 829);

            var file = new MemoryPagedFile(8);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
            file.Pages[2] = NodeBuilder.Leaf(0)
                .AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(57), cell0Data))
                .ToBytes();

            using (var container = new IdxContainer(file))
            {
                var rootBase = container.WriteTree(0);
                Assert.IsNotNull(rootBase, "Node to write");
                Assert.IsInstanceOfType(rootBase, typeof(IdxLeaf), "Root type");
                var root = (IdxLeaf)rootBase;
                var cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(857), cell1Data);
                var overflow = container.CreateOverflow(0);
                overflow.WriteData(cell1Data, cell.ValueLength);
                cell.OverflowPage = overflow.PageNumber;
                root.AddCell(cell);
                container.CommitWrite(0);
            }

            {
                CollectionAssert.AreEquivalent(new int[] { 1, 2, 3 }, file.WrittenPages.ToList(), "Written pages");
                var leaf = new IdxLeaf(file.Pages[2]);
                Assert.AreEqual(2, leaf.CellsCount, "Leaf cells count");
                Assert.AreEqual(IdxKey.FromInteger(857), leaf.GetCell(1).Key, "Leaf cell 0 key");
                Assert.AreEqual(3, leaf.GetCell(1).OverflowPage, "cell 0 overflow page");
                Assert.AreEqual(116, leaf.GetCell(1).ValueLength, "cell 0 value length");
                Assert.AreEqual(1, leaf.GetCell(1).OverflowLength, "cell 0 overflow count");

                var overflow = new IdxOverflow(file.Pages[3]);
                Assert.AreEqual(829 - 116, overflow.LengthInPage, "overflow length");

                var buffer = new byte[overflow.LengthInPage + 116];
                Array.Copy(leaf.GetCell(1).ValueBytes, buffer, 116);
                overflow.ReadData(buffer, 116);
                CollectionAssert.AreEqual(cell1Data, buffer, "Data");
            }
        }

        [TestMethod]
        public void SplitLeavesAndAddInterior()
        {
            var random = new Random(3541324);
            var file = new MemoryPagedFile(8);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
            var originalLeafBuilder = NodeBuilder.Leaf(0);
            var cells = new List<IdxCell>();
            for (int i = 0; i < 31; i++)
            {
                var value = ContainerTestUtilities.CreateBytes(random, 116);
                var cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(i), value);
                cells.Add(cell);
                originalLeafBuilder.AddCell(cell);
            }
            var addedCell = IdxCell.CreateLeafCell(IdxKey.FromInteger(32), ContainerTestUtilities.CreateBytes(random, 116));
            cells.Add(addedCell);
            file.Pages[2] = originalLeafBuilder.ToBytes();

            using (var container = new IdxContainer(file))
            {
                var leaf1 = (IdxLeaf)container.WriteTree(0);
                var leaf2 = container.CreateLeaf(0);
                var key = leaf1.Split(leaf2, addedCell);
                var interior = container.CreateInterior(0);
                interior.LeftmostPage = leaf1.PageNumber;
                interior.AddCell(IdxCell.CreateInteriorCell(key, leaf2.PageNumber));
                container.SetTreeRoot(0, interior);
                Assert.AreSame(leaf2, container.GetNode(0, leaf2.PageNumber), "Already used page");
                container.CommitWrite(0);
            }

            {
                CollectionAssert.AreEquivalent(new int[] { 0, 1, 2, 3, 4 }, file.WrittenPages.ToList(), "Written pages");
                Assert.AreEqual(4, new IdxHeader(file.Pages[0]).GetTreeRoot(0));
                Assert.AreEqual(3, new IdxFreeList(file.Pages[1]).Length, "Free pages");
                var interior = new IdxInterior(file.Pages[4]);
                Assert.AreEqual(1, interior.CellsCount, "Interior cells count");
                Assert.AreEqual(2, interior.LeftmostPage, "Leftmost page");
                Assert.AreEqual(3, interior.GetCell(0).ChildPage, "Child page");
                var leaf1 = new IdxLeaf(file.Pages[2]);
                var leaf2 = new IdxLeaf(file.Pages[3]);
                var newCells = new List<IdxCell>();
                for (int i = 0; i < leaf1.CellsCount; i++)
                    newCells.Add(leaf1.GetCell(i));
                for (int i = 0; i < leaf2.CellsCount; i++)
                    newCells.Add(leaf2.GetCell(i));
                Assert.AreEqual(cells.Count, newCells.Count, "Cells total count");
                for (int i = 0; i < newCells.Count; i++)
                {
                    Assert.AreEqual(cells[i].Key, newCells[i].Key, "Cell {0} key", i);
                    CollectionAssert.AreEquivalent(cells[i].ValueBytes, newCells[i].ValueBytes, "Cell {0} value", i);
                }
            }
        }

        [TestMethod]
        public void DropOverflowPage()
        {
            var random = new Random(54657);
            var longValue = ContainerTestUtilities.CreateBytes(random, 6754);
            var shorterValue = ContainerTestUtilities.CreateBytes(random, 840);
            var file = new MemoryPagedFile(8);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 3);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 8, 7, 6, 5);
            file.Pages[2] = ContainerTestUtilities.CreateOverflow(4, 116, longValue);
            file.Pages[3] = NodeBuilder.Leaf(0).AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(5457), longValue)).ToBytes();
            file.Pages[4] = ContainerTestUtilities.CreateOverflow(0, 116 + IdxOverflow.Capacity, longValue);

            using (var container = new IdxContainer(file))
            {
                var leaf = (IdxLeaf)container.WriteTree(0);
                leaf.GetCell(0).ChangeValue(shorterValue);
                var overflow1 = container.GetOverflow(0, 2);
                overflow1.WriteData(shorterValue, leaf.GetCell(0).ValueLength);
                overflow1.Next = 0;
                var overflow2 = container.GetOverflow(0, 4);
                container.Delete(0, 4);
                container.CommitWrite(0);
            }
            {
                var overflow1 = new IdxOverflow(file.Pages[2]);
                Assert.AreEqual(0, overflow1.Next, "Overflow next");
                Assert.AreEqual(840 - 116, overflow1.LengthInPage, "Overflow length");
                var leaf = new IdxLeaf(file.Pages[3]);
                Assert.AreEqual(1, leaf.GetCell(0).OverflowLength, "Overflow count");
                CollectionAssert.AreEqual(shorterValue.Take(116).ToArray(), leaf.GetCell(0).ValueBytes, "Cell value");
                var freelist = new IdxFreeList(file.Pages[1]);
                Assert.AreEqual(4, freelist.Alloc(), "Second overflow page was freed");
                CollectionAssert.AreEquivalent(new int[] { 1, 2, 3 }, file.WrittenPages.ToList(), "Written pages");
            }
        }
    }
}
