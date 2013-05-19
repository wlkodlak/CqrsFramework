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
    }
}
