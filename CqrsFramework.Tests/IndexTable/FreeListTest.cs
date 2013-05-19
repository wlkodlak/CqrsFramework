using System;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;
using System.Collections.Generic;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class FreeListTest
    {
        [TestMethod]
        public void LoadEmptyList()
        {
            IdxFreeList list = new IdxFreeList((byte[])null);
            list.PageNumber = 443;
            Assert.AreEqual(443, list.PageNumber);
            Assert.AreEqual(0, list.Next);
            Assert.AreEqual(0, list.Length);
            Assert.IsFalse(list.IsDirty);
            Assert.IsFalse(list.IsFull);
            Assert.IsTrue(list.IsEmpty);
            Assert.IsTrue(list.IsLast);
        }

        [TestMethod]
        public void LoadFullList()
        {
            var data = CreateCorrectData(0, GeneratePagesList(1022));
            IdxFreeList list = new IdxFreeList(data);
            Assert.AreEqual(0, list.Next);
            Assert.AreEqual(1022, list.Length);
            Assert.IsFalse(list.IsDirty);
            Assert.IsTrue(list.IsFull);
            Assert.IsFalse(list.IsEmpty);
            Assert.IsTrue(list.IsLast);
        }

        [TestMethod]
        public void LoadHalfEmptyList()
        {
            var contents = GeneratePagesList(520);
            var data = CreateCorrectData(4, contents);
            IdxFreeList list = new IdxFreeList(data);
            Assert.AreEqual(4, list.Next);
            Assert.AreEqual(520, list.Length);
            Assert.IsFalse(list.IsDirty);
            Assert.IsFalse(list.IsFull);
            Assert.IsFalse(list.IsEmpty);
            Assert.IsFalse(list.IsLast);
        }

        [TestMethod]
        public void AddingToHalfFull()
        {
            var contents = GeneratePagesList(442);
            var data = CreateCorrectData(222, contents);
            IdxFreeList list = new IdxFreeList(data);
            var dirty = new AssertDirtyChanged(list);
            list.Add(2933);
            contents.Add(2933);
            byte[] serialized = list.Save();
            CollectionAssert.AreEqual(CreateCorrectData(222, contents), serialized);
            Assert.AreEqual(222, list.Next);
            dirty.AssertFalse();
            Assert.IsFalse(list.IsFull);
            Assert.IsFalse(list.IsEmpty);
            Assert.IsFalse(list.IsLast);
        }

        [TestMethod]
        public void AddToAlmostFull()
        {
            var contents = GeneratePagesList(1021);
            var data = CreateCorrectData(0, contents);
            IdxFreeList list = new IdxFreeList(data);
            var dirtyChanged = new AssertDirtyChanged(list);
            list.Add(2200);
            contents.Add(2200);
            Assert.AreEqual(0, list.Next);
            Assert.IsTrue(list.IsDirty);
            Assert.IsTrue(list.IsFull);
            Assert.IsFalse(list.IsEmpty);
            Assert.IsTrue(list.IsLast);
            dirtyChanged.AssertTrue();
        }

        [TestMethod]
        public void AllocFromAlmostEmpty()
        {
            var data = CreateCorrectData(4, new int[] { 2930 });
            var list = new IdxFreeList(data);
            var dirty = new AssertDirtyChanged(list);
            int allocated = list.Alloc();
            Assert.AreEqual(2930, allocated);
            Assert.AreEqual(4, list.Next);
            dirty.AssertTrue();
            Assert.IsFalse(list.IsFull);
            Assert.IsTrue(list.IsEmpty);
            Assert.IsFalse(list.IsLast);
        }

        [TestMethod]
        public void AllocFromHalfFull()
        {
            var contents = GeneratePagesList(322);
            var lastPage = contents[321];
            var data = CreateCorrectData(4, contents);
            var list = new IdxFreeList(data);
            int allocated = list.Alloc();
            contents.RemoveAt(contents.Count - 1);
            byte[] serialized = list.Save();
            Assert.AreEqual(lastPage, allocated);
            CollectionAssert.AreEqual(CreateCorrectData(4, contents), serialized);
            Assert.AreEqual(4, list.Next);
            Assert.IsFalse(list.IsDirty);
            Assert.IsFalse(list.IsFull);
            Assert.IsFalse(list.IsEmpty);
            Assert.IsFalse(list.IsLast);
        }

        private List<int> GeneratePagesList(int count)
        {
            var result = new List<int>(1022);
            for (int i = 0; i < count; i++)
            {
                int highBits = (i >> 9);
                int lowBits = (i >> 5) & 0xF;
                int medBits = i & 0x1F;
                result.Add((highBits << 9) | (medBits << 4) | (lowBits));
            }
            return result;
        }

        private byte[] CreateCorrectData(int next, IList<int> freePages)
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer), Encoding.ASCII, false))
            {
                writer.Write(next);
                writer.Write(freePages.Count);
                foreach (int page in freePages)
                    writer.Write(page);
            }
            return buffer;
        }
    }
}
