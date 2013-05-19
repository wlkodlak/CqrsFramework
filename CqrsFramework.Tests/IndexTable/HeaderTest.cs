using System;
using System.IO;
using System.Text;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class HeaderTest
    {
        [TestMethod]
        public void CanLoadEmptyHeader()
        {
            IdxHeader header = new IdxHeader((byte[])null);
            Assert.AreEqual(0, header.FreePagesList);
            Assert.AreEqual(0, header.TotalPagesCount);
            for (int i = 0; i < 16; i++)
                Assert.AreEqual(0, header.GetTreeRoot(i));
            Assert.IsFalse(header.IsDirty);
        }

        [TestMethod]
        public void LoadHeaderWithTwoTrees()
        {
            var data = GetCorrectContents(1, 64, new[] { 4, 15 });
            IdxHeader header = new IdxHeader(data);
            Assert.AreEqual(1, header.FreePagesList);
            Assert.AreEqual(64, header.TotalPagesCount);
            Assert.AreEqual(4, header.GetTreeRoot(0));
            Assert.AreEqual(15, header.GetTreeRoot(1));
            for (int i = 2; i < 16; i++)
                Assert.AreEqual(0, header.GetTreeRoot(i));
            Assert.IsFalse(header.IsDirty);
        }

        [TestMethod]
        public void CreateHeaderWithOneTree()
        {
            IdxHeader header = new IdxHeader(null);
            header.FreePagesList = 2;
            header.TotalPagesCount = 128;
            header.SetTreeRoot(0, 5);
            Assert.AreEqual(2, header.FreePagesList);
            Assert.AreEqual(128, header.TotalPagesCount);
            Assert.AreEqual(5, header.GetTreeRoot(0));
            for (int i = 1; i < 16; i++)
                Assert.AreEqual(0, header.GetTreeRoot(i));
            Assert.IsTrue(header.IsDirty);
        }

        [TestMethod]
        public void SaveCreatedHeaderWithOneTree()
        {
            IdxHeader header = new IdxHeader(null);
            header.FreePagesList = 2;
            header.TotalPagesCount = 128;
            header.SetTreeRoot(0, 5);
            byte[] serialized = header.Save();
            Assert.IsFalse(header.IsDirty);
            var expected = GetCorrectContents(2, 128, new[] { 5 });
            AssertBytes(expected, serialized);
        }

        [TestMethod]
        public void SaveUpdatedHeaderWithAddedTree()
        {
            var data = GetCorrectContents(3, 32, new[] { 2, 3 });
            
            IdxHeader header = new IdxHeader(data);
            header.FreePagesList = 22;
            header.TotalPagesCount = 48;
            header.SetTreeRoot(1, 4);
            header.SetTreeRoot(4, 29);
            var serialized = header.Save();

            Assert.IsFalse(header.IsDirty);
            var expected = GetCorrectContents(22, 48, new[] { 2, 4, 0, 0, 29 });
            AssertBytes(expected, serialized);
        }

        [TestMethod]
        public void InvalidHeaderThrows()
        {
            try
            {
                var data = GetCorrectContents(2, 4, new[] { 4, 2 });
                data[1] = 0x22;
                new IdxHeader(data);
                Assert.Fail("Expected InvalidDataException");
            }
            catch (InvalidDataException)
            {
            }
        }

        private void AssertBytes(byte[] expected, byte[] serialized)
        {
            CollectionAssert.AreEqual(expected, serialized);
        }

        private byte[] GetCorrectContents(int freePagesList, int totalPagesCount, int[] treeRoots)
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer), Encoding.ASCII, false))
            {
                writer.Write(new byte[4] { 0x49, 0x58, 0x54, 0x4c });   // magic
                writer.Write(freePagesList);
                writer.Write(totalPagesCount);
                writer.Write(0);
                writer.Write(new byte[16]);     // reserved space
                for (int i = 0; i < treeRoots.Length; i++)
                    writer.Write(treeRoots[i]);
                for (int i = treeRoots.Length; i < 16; i++)
                    writer.Write(0);
            }
            return buffer;
        }
    }
}
