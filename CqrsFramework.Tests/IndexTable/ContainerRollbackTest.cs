using System;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class ContainerRollbackTest
    {
        [TestMethod]
        public void CreateLongLeaf()
        {
            var savedData = new byte[8726];
            new Random(657).NextBytes(savedData);

            var file = new MemoryPagedFile(4);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(2, 4);
            file.Pages[2] = ContainerTestUtilities.CreateFreeList(0, 3, 1);

            using (var container = new IdxContainer(file))
            {
                container.WriteTree(0);
                var cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(8547), savedData, 4096);
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

                container.RollbackWrite(0);
            }
            {
                CollectionAssert.AreEquivalent(new int[] { 0, 2 }, file.WrittenPages.ToList(), "Written pages");
                var freelist = new IdxFreeList(file.Pages[2], 4096);
                var freePages = Enumerable.Range(0, freelist.Length).Select(i => freelist.Alloc()).ToList();
                CollectionAssert.AreEquivalent(new int[] { 1, 3, 4, 5, 6, 7 }, freePages, "Free pages");
                var header = new IdxHeader(file.Pages[0], 4096);
                Assert.AreEqual(0, header.GetTreeRoot(0), "Tree 0 root");
            }
        }

        [TestMethod]
        public void TestLockWriteWrite()
        {
            try
            {
                var file = new MemoryPagedFile(8);
                file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
                file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
                file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
                using (var container = new IdxContainer(file))
                {
                    container.WriteTree(0);
                    container.WriteTree(0);
                }
                Assert.Fail("Expected IdxLockException");
            }
            catch (IdxLockException)
            {
            }
        }

        [TestMethod]
        public void TestLockWriteRead()
        {
            try
            {
                var file = new MemoryPagedFile(8);
                file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
                file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
                file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
                using (var container = new IdxContainer(file))
                {
                    container.WriteTree(0);
                    container.ReadTree(0);
                }
                Assert.Fail("Expected IdxLockException");
            }
            catch (IdxLockException)
            {
            }
        }

        [TestMethod]
        public void TestLockReadWrite()
        {
            try
            {
                var file = new MemoryPagedFile(8);
                file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
                file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
                file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
                using (var container = new IdxContainer(file))
                {
                    container.ReadTree(0);
                    container.WriteTree(0);
                }
                Assert.Fail("Expected IdxLockException");
            }
            catch (IdxLockException)
            {
            }
        }

        [TestMethod]
        public void TestLockReadRead()
        {
            var file = new MemoryPagedFile(8);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
            file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
            using (var container = new IdxContainer(file))
            {
                container.ReadTree(0);
                container.ReadTree(0);
                container.UnlockRead(0);
                container.UnlockRead(0);
            }
        }

        [TestMethod]
        public void TestLockReadAfterWrite()
        {
            var file = new MemoryPagedFile(8);
            file.Pages[0] = ContainerTestUtilities.CreateHeader(1, 8, 2);
            file.Pages[1] = ContainerTestUtilities.CreateFreeList(0, 7, 6, 5, 4, 3);
            file.Pages[2] = NodeBuilder.Leaf(0).ToBytes();
            using (var container = new IdxContainer(file))
            {
                container.WriteTree(0);
                container.CommitWrite(0);
                container.ReadTree(0);
                container.UnlockRead(0);
                container.WriteTree(0);
                container.RollbackWrite(0);
                container.ReadTree(0);
                container.UnlockRead(0);
            }
        }
    }
}
