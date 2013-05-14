using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable.OverflowPage
{
    [TestClass]
    public class IndexTableOverflowTest
    {
        [TestMethod]
        public void CreateSingleHalfFull()
        {
            var data = CreateData(49083, 582);

            IdxOverflow page = new IdxOverflow(null);
            int written = page.WriteData(data, 48);

            Assert.AreEqual(534, written);
            Assert.IsTrue(page.IsDirty);
            Assert.AreEqual(534, page.LengthInPage);
            Assert.AreEqual(0, page.Next);
            Assert.IsFalse(page.HasNextPage);
            Assert.IsFalse(page.NeedsNextPage);

            var readBuffer = new byte[4096];
            var readCount = page.ReadData(readBuffer, 0);

            Assert.AreEqual(534, readCount);
            CollectionAssert.AreEqual(data.Skip(48).ToArray(), readBuffer.Take(534).ToArray());
        }

        [TestMethod]
        public void CreateTooLong()
        {
            var data = CreateData(39924, 8231);
            var readBuffer = new byte[5000];
            for (int i = 0; i < readBuffer.Length; i++)
                readBuffer[i] = 14;

            IdxOverflow page = new IdxOverflow(null);
            int written = page.WriteData(data, 120);

            Assert.AreEqual(4088, IdxOverflow.Capacity);
            Assert.AreEqual(IdxOverflow.Capacity, written);
            Assert.AreEqual(IdxOverflow.Capacity, page.LengthInPage);
            Assert.IsTrue(page.IsDirty);
            Assert.IsFalse(page.HasNextPage);
            Assert.IsTrue(page.NeedsNextPage);

            int readCount = page.ReadData(readBuffer, 120);

            Assert.AreEqual(IdxOverflow.Capacity, readCount);
            Assert.IsTrue(readBuffer.Take(120).All(b => b == 14));
            CollectionAssert.AreEqual(data.Skip(120).Take(IdxOverflow.Capacity).ToArray(), readBuffer.Skip(120).Take(IdxOverflow.Capacity).ToArray());
            Assert.IsTrue(readBuffer.Skip(120 + IdxOverflow.Capacity).All(b => b == 14));
        }

        [TestMethod]
        public void SaveLongCreated()
        {
            var data = CreateData(39924, 8231);

            IdxOverflow page = new IdxOverflow(null);
            page.WriteData(data, 120);
            page.Next = 37;
            byte[] serialized = page.Save();

            Assert.IsFalse(page.IsDirty);
            Assert.IsTrue(page.HasNextPage);
            Assert.AreEqual(37, page.Next);
            var expected = CreateCorrectBytes(37, 120, data);
            CollectionAssert.AreEqual(expected, serialized);
        }

        [TestMethod]
        public void LoadShort()
        {
            var data = CreateData(94423, 1042);
            var bytes = CreateCorrectBytes(0, 0, data);
            
            IdxOverflow page = new IdxOverflow(bytes);

            Assert.AreEqual(1042, page.LengthInPage);
            Assert.IsFalse(page.HasNextPage);
            Assert.IsFalse(page.IsDirty);
            Assert.IsFalse(page.NeedsNextPage);
            Assert.AreEqual(0, page.Next);

            var read = new byte[1042];
            page.ReadData(read, 0);
            CollectionAssert.AreEqual(data, read);
        }

        [TestMethod]
        public void LoadLongWithNext()
        {
            var data = CreateData(94423, 9999);
            var bytes = CreateCorrectBytes(388, 0, data);

            IdxOverflow page = new IdxOverflow(bytes);

            Assert.AreEqual(IdxOverflow.Capacity, page.LengthInPage);
            Assert.IsTrue(page.HasNextPage);
            Assert.IsFalse(page.IsDirty);
            Assert.IsTrue(page.NeedsNextPage);
            Assert.AreEqual(388, page.Next);

            var read = new byte[IdxOverflow.Capacity];
            page.ReadData(read, 0);
            CollectionAssert.AreEqual(data.Take(IdxOverflow.Capacity).ToArray(), read);
        }

        [TestMethod]
        public void LoadShortWithNext()
        {
            var data = CreateData(94423, 1042);
            var bytes = CreateCorrectBytes(223, 0, data);

            IdxOverflow page = new IdxOverflow(bytes);

            Assert.AreEqual(1042, page.LengthInPage);
            Assert.IsTrue(page.HasNextPage);
            Assert.IsFalse(page.IsDirty);
            Assert.IsFalse(page.NeedsNextPage);
            Assert.AreEqual(223, page.Next);

            var read = new byte[1042];
            page.ReadData(read, 0);
            CollectionAssert.AreEqual(data, read);
        }

        [TestMethod]
        public void UpdateLong()
        {
            var data = CreateData(94423, 4100);
            var bytes = CreateCorrectBytes(388, 0, data);

            var changed = CreateData(3843, 4200);

            IdxOverflow page = new IdxOverflow(bytes);
            int written = page.WriteData(changed, 0);

            Assert.IsTrue(page.NeedsNextPage);
            Assert.IsTrue(page.HasNextPage);
            Assert.IsTrue(page.IsDirty);
            Assert.AreEqual(IdxOverflow.Capacity, page.LengthInPage);
            Assert.AreEqual(388, page.Next);

            var readBuffer = new byte[IdxOverflow.Capacity];
            page.ReadData(readBuffer, 0);
            CollectionAssert.AreEqual(changed.Take(IdxOverflow.Capacity).ToArray(), readBuffer);
        }

        [TestMethod]
        public void SaveUpdateLongtWithNext()
        {
            var data = CreateData(94423, 4100);
            var bytes = CreateCorrectBytes(388, 0, data);

            var changed = CreateData(3843, 400);

            IdxOverflow page = new IdxOverflow(bytes);
            page.WriteData(changed, 0);
            byte[] serialized = page.Save();

            Assert.IsFalse(page.IsDirty);
            var expected = CreateCorrectBytes(388, 0, changed);
            CollectionAssert.AreEqual(expected, serialized);
        }

        private byte[] CreateData(int seed, int length)
        {
            var rnd = new Random(seed);
            var data = new byte[length];
            rnd.NextBytes(data);
            return data;
        }

        private byte[] CreateCorrectBytes(int next, int skip, byte[] data)
        {
            var bytes = new byte[PagedFile.PageSize];
            int remaining = data.Length - skip;
            int capacity = PagedFile.PageSize - 8;
            short localSize = (short)Math.Min(remaining, capacity);
            byte flags = remaining > capacity ? (byte)1 : (byte)0;
            using (var writer = new BinaryWriter(new MemoryStream(bytes), Encoding.ASCII, false))
            {
                writer.Write(next);
                writer.Write(localSize);
                writer.Write(flags);
                writer.Write((byte)0);
                writer.Write(data, skip, localSize);
            }
            return bytes;
        }
    }
}
