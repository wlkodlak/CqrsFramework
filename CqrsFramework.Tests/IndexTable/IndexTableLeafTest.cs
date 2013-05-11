using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableLeafTest
    {
        [TestMethod]
        public void CreateEmpty()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            Assert.AreEqual(0, leaf.Next);
            Assert.AreEqual(0, leaf.CellsCount);
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void AddCellToEmptyLeaf()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            IdxCell cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(482), null);
            leaf.AddCell(cell);
            Assert.AreEqual(1, leaf.CellsCount);
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
            Assert.AreEqual(IdxKey.FromInteger(482), leaf.GetCell(0).Key);
        }

        [TestMethod]
        public void MediumSizedLeafUsingBigCells()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 8; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[116]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void SmallSizedLeafUsingTinyCells()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 16; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void MediumSizedLeafUsingTinyCells()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 64; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void FullSizedLeafUsingBigCells()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 31; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[116]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsTrue(leaf.IsFull);
        }

        [TestMethod]
        public void FullSizedLeafUsingTinyCells()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 247; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsTrue(leaf.IsFull);
        }

        [TestMethod]
        public void CellsHaveOrdinalNumber()
        {
            IdxLeaf leaf = new IdxLeaf(null);
            for (int i = 0; i < 64; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[16]));
            for (int i = 0; i < leaf.CellsCount; i++)
                Assert.AreEqual(i, leaf.GetCell(i).Ordinal);
        }

        [TestMethod]
        public void LoadingMediumSizedLeafFromBytes()
        {
            var data = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(data)))
            {
                writer.Write((byte)1);
                writer.Write((byte)16);
                writer.Write((byte)0);
                writer.Write((byte)0);
                writer.Write(0);
                writer.Write(new byte[8]);

                WriteCellInfo(writer, "hdr", 483, 16, 0);
                WriteCellInfo(writer, "kdjif", 3829, 12000, 4938);
                WriteCellInfo(writer, "ewrf", 32, 4232, 423);
                WriteCellInfo(writer, "vsdf", 8743, 623, 223);
                WriteCellInfo(writer, "lddkf", 543, 5233, 884);
                WriteCellInfo(writer, "owcjdaa", 223, 3112, 854);
                WriteCellInfo(writer, "cd", 543, 3322, 65);
                WriteCellInfo(writer, "fea", 6443, 419, 45);
                WriteCellInfo(writer, "ccsf", 2234, 8832, 223);
                WriteCellInfo(writer, "oodkc", 9382, 16, 0);
                WriteCellInfo(writer, "aca", 6334, 16, 0);
                WriteCellInfo(writer, "casf", 9382, 16, 0);
                WriteCellInfo(writer, "wcdf", 9842, 16, 0);
                WriteCellInfo(writer, "idhr", 2699, 16, 0);
                WriteCellInfo(writer, "isfue", 385, 16, 0);
                WriteCellInfo(writer, "pwoic", 112, 16, 544);
            }

            IdxLeaf leaf = new IdxLeaf(data);

            Assert.AreEqual(16, leaf.CellsCount);
            Assert.IsFalse(leaf.IsFull);
            Assert.IsFalse(leaf.IsSmall);

            Assert.IsTrue(leaf.GetCell(1).IsLeaf);
            Assert.AreEqual(IdxKey.FromString("kdjif"), leaf.GetCell(1).Key);

            Assert.AreEqual(1, leaf.GetCell(1).Ordinal);
            Assert.AreEqual(3, leaf.GetCell(1).OverflowLength);
            Assert.AreEqual(4938, leaf.GetCell(1).OverflowPage);
            CollectionAssert.AreEqual(GenerateValue(3829, 115), leaf.GetCell(1).ValueBytes);
            Assert.AreEqual(128, leaf.GetCell(1).CellSize);

            Assert.IsTrue(leaf.GetCell(9).IsLeaf);
            Assert.AreEqual(IdxKey.FromString("oodkc"), leaf.GetCell(9).Key);
            Assert.AreEqual(9, leaf.GetCell(9).Ordinal);
            Assert.AreEqual(0, leaf.GetCell(9).OverflowLength);
            Assert.AreEqual(0, leaf.GetCell(9).OverflowPage);
            CollectionAssert.AreEqual(GenerateValue(9382, 3), leaf.GetCell(9).ValueBytes);
            Assert.AreEqual(16, leaf.GetCell(9).CellSize);
        }

        private void WriteCellInfo(BinaryWriter writer, string id, int seed, int length, int oflPage)
        {
            byte keyLength = (byte)id.Length;
            int valueLength = length - id.Length - 8;
            byte localLength = (byte)Math.Min(valueLength, 120 - keyLength);
            int remainingLength = valueLength - localLength;
            short overflowPages = (short)((remainingLength + 4087) / 4088);
            byte[] key = Encoding.ASCII.GetBytes(id);
            byte[] localValue = GenerateValue(seed, localLength);

            writer.Write(keyLength);
            writer.Write(localLength);
            writer.Write(overflowPages);
            writer.Write(oflPage);
            writer.Write(key);
            writer.Write(localValue);
        }

        private byte[] GenerateValue(int seed, int length)
        {
            var result = new byte[length];
            new Random(seed).NextBytes(result);
            return result;
        }

        /*
         * Loading from bytes - 
         * Saving
         * Removing cells
         * Finding nearest cell by key
         */
    }
}
