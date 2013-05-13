﻿using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableNodeTest
    {
        [TestMethod]
        public void CreateEmpty()
        {
            IdxNode leaf = new IdxNode(null);
            Assert.AreEqual(0, leaf.Next);
            Assert.AreEqual(0, leaf.CellsCount);
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
            Assert.IsFalse(leaf.IsDirty);
        }

        [TestMethod]
        public void AddCellToEmptyLeaf()
        {
            IdxNode leaf = new IdxNode(null);
            IdxCell cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(482), null);
            leaf.AddCell(cell);
            Assert.AreEqual(1, leaf.CellsCount);
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
            Assert.AreEqual(IdxKey.FromInteger(482), leaf.GetCell(0).Key);
            Assert.IsTrue(leaf.IsDirty);
        }

        [TestMethod]
        public void MediumSizedLeafUsingBigCells()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 8; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[116]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
            Assert.IsTrue(leaf.IsDirty);
        }

        [TestMethod]
        public void SmallSizedLeafUsingTinyCells()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 16; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsTrue(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void MediumSizedLeafUsingTinyCells()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 64; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsFalse(leaf.IsFull);
        }

        [TestMethod]
        public void FullSizedLeafUsingBigCells()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 31; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[116]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsTrue(leaf.IsFull);
        }

        [TestMethod]
        public void FullSizedLeafUsingTinyCells()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 247; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[4]));
            Assert.IsFalse(leaf.IsSmall);
            Assert.IsTrue(leaf.IsFull);
        }

        [TestMethod]
        public void CellsHaveOrdinalNumber()
        {
            IdxNode leaf = new IdxNode(null);
            for (int i = 0; i < 64; i++)
                leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), new byte[16]));
            for (int i = 0; i < leaf.CellsCount; i++)
                Assert.AreEqual(i, leaf.GetCell(i).Ordinal);
        }

        [TestMethod]
        public void LoadingMediumSizedLeafFromBytes()
        {
            var data = SampleData();

            IdxNode leaf = new IdxNode(data);

            Assert.AreEqual(16, leaf.CellsCount);
            Assert.AreEqual(8109, leaf.Next);
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

        private byte[] SampleData()
        {
            var data = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(data)))
            {
                writer.Write((byte)1);
                writer.Write((byte)16);
                writer.Write((byte)0);
                writer.Write((byte)0);
                writer.Write(8109);
                writer.Write(new byte[8]);

                WriteCellInfo(writer, "abc", 483, 16, 0);               // 0
                WriteCellInfo(writer, "kdjif", 3829, 12000, 4938);
                WriteCellInfo(writer, "kdzrf", 32, 4232, 423);          // 2
                WriteCellInfo(writer, "lsdf", 8743, 623, 223);
                WriteCellInfo(writer, "lzdkf", 543, 5233, 884);         // 4
                WriteCellInfo(writer, "mwcjdaa", 223, 3112, 854);
                WriteCellInfo(writer, "mz", 543, 3322, 65);             // 6
                WriteCellInfo(writer, "naa", 6443, 419, 45);
                WriteCellInfo(writer, "ncsf", 2234, 8832, 223);         // 8
                WriteCellInfo(writer, "oodkc", 9382, 16, 0);
                WriteCellInfo(writer, "opa", 6334, 16, 0);              // 10
                WriteCellInfo(writer, "pasf", 9382, 16, 0);
                WriteCellInfo(writer, "rcdf", 9842, 16, 0);             // 12
                WriteCellInfo(writer, "sdhr", 2699, 16, 0);
                WriteCellInfo(writer, "ssfue", 385, 16, 0);             // 14
                WriteCellInfo(writer, "twoic", 112, 16, 544);
            }
            return data;
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

        [TestMethod]
        public void FindingNearestCell()
        {
            var data = SampleData();
            var leaf = new IdxNode(data);
            Assert.AreEqual(0, leaf.FindByKey(IdxKey.FromString("abc")).Ordinal);
            Assert.AreEqual(8, leaf.FindByKey(IdxKey.FromString("ncsf")).Ordinal);
            Assert.AreEqual(11, leaf.FindByKey(IdxKey.FromString("opat")).Ordinal);
            Assert.AreEqual(12, leaf.FindByKey(IdxKey.FromString("rcd")).Ordinal);
            Assert.IsNull(leaf.FindByKey(IdxKey.FromString("twz")));
        }

        [TestMethod]
        public void RemoveCell()
        {
            var data = SampleData();
            var leaf = new IdxNode(data);
            leaf.RemoveCell(10);
            Assert.IsTrue(leaf.IsDirty);
            Assert.AreEqual(15, leaf.CellsCount);
            Assert.AreEqual(10, leaf.GetCell(10).Ordinal);
            Assert.AreEqual(IdxKey.FromString("pasf"), leaf.GetCell(10).Key);
        }

        [TestMethod]
        public void RemovingCellsDecreasesUsedSize()
        {
            var data = SampleData();
            var leaf = new IdxNode(data);
            for (int i = 0; i < 14; i++)
                leaf.RemoveCell(0);
            Assert.IsTrue(leaf.IsSmall);
        }

        [TestMethod]
        public void SavingBytes()
        {
            var leaf = new IdxNode(null);
            leaf.Next = 8109;

            GenerateCell(leaf, "abc", 483, 16, 0);               // 0
            GenerateCell(leaf, "kdjif", 3829, 12000, 4938);
            GenerateCell(leaf, "kdzrf", 32, 4232, 423);
            GenerateCell(leaf, "lsdf", 8743, 623, 223);
            GenerateCell(leaf, "lzdkf", 543, 5233, 884);         // 4
            GenerateCell(leaf, "mwcjdaa", 223, 3112, 854);
            GenerateCell(leaf, "mz", 543, 3322, 65);
            GenerateCell(leaf, "naa", 6443, 419, 45);
            GenerateCell(leaf, "ncsf", 2234, 8832, 223);         // 8
            GenerateCell(leaf, "oodkc", 9382, 16, 0);
            GenerateCell(leaf, "opa", 6334, 16, 0);
            GenerateCell(leaf, "pasf", 9382, 16, 0);
            GenerateCell(leaf, "rcdf", 9842, 16, 0);             // 12
            GenerateCell(leaf, "sdhr", 2699, 16, 0);
            GenerateCell(leaf, "ssfue", 385, 16, 0);
            GenerateCell(leaf, "twoic", 112, 16, 544);

            var saved = leaf.Save();
            var expected = SampleData();
            Assert.IsFalse(leaf.IsDirty);
            CollectionAssert.AreEqual(expected, saved);
        }

        private void GenerateCell(IdxNode leaf, string id, int seed, int length, int oflPage)
        {
            int valueLength = length - id.Length - 8;
            var cell = IdxCell.CreateLeafCell(IdxKey.FromString(id), GenerateValue(seed, valueLength));
            cell.OverflowPage = oflPage;
            leaf.AddCell(cell);
        }

        [TestMethod]
        public void AddingKeepsCellsSorted()
        {
            var leaf = new IdxNode(null);
            leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(153), null));
            leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(3), null));
            leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(53), null));
            leaf.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(13), null));
            Assert.AreEqual(IdxKey.FromInteger(3), leaf.GetCell(0).Key);
            Assert.AreEqual(IdxKey.FromInteger(13), leaf.GetCell(1).Key);
            Assert.AreEqual(IdxKey.FromInteger(53), leaf.GetCell(2).Key);
            Assert.AreEqual(IdxKey.FromInteger(153), leaf.GetCell(3).Key);
        }

        [TestMethod]
        public void UpdatingNextDirties()
        {
            var leaf = new IdxNode(SampleData());
            leaf.Next = 4874;
            Assert.IsTrue(leaf.IsDirty);
        }

        [TestMethod]
        public void PageNumber()
        {
            var leaf = new IdxNode(null);
            leaf.PageNumber = 382;
            Assert.IsFalse(leaf.IsDirty);
        }

        [TestMethod]
        public void SplitAtKey125Size16()
        {
            var leaf = PrepareSplitLeaf();
            var newPage = new IdxNode(null);
            newPage.PageNumber = 888;
            IdxCell addedCell = IdxCell.CreateLeafCell(IdxKey.FromInteger(125), new byte[4]);
            IdxKey splitKey = leaf.SplitLeaf(newPage, addedCell);
            Assert.AreEqual(IdxKey.FromInteger(430), splitKey);
            Assert.AreEqual(43, leaf.CellsCount);
            Assert.AreEqual(45, newPage.CellsCount);
            Assert.AreEqual(IdxKey.FromInteger(125), leaf.GetCell(12).Key);
        }

        [TestMethod]
        public void SplitAtKey125Size128()
        {
            var leaf = PrepareSplitLeaf();
            var newPage = new IdxNode(null);
            newPage.PageNumber = 888;
            IdxCell addedCell = IdxCell.CreateLeafCell(IdxKey.FromInteger(125), new byte[116]);
            IdxKey splitKey = leaf.SplitLeaf(newPage, addedCell);
            Assert.AreEqual(IdxKey.FromInteger(360), splitKey);
            Assert.AreEqual(36, leaf.CellsCount);
            Assert.AreEqual(52, newPage.CellsCount);
            Assert.AreEqual(IdxKey.FromInteger(125), leaf.GetCell(12).Key);
        }

        [TestMethod]
        public void SplitAtKey435Size128()
        {
            var leaf = PrepareSplitLeaf();
            var newPage = new IdxNode(null);
            newPage.PageNumber = 888;
            IdxCell addedCell = IdxCell.CreateLeafCell(IdxKey.FromInteger(435), new byte[116]);
            IdxKey splitKey = leaf.SplitLeaf(newPage, addedCell);
            Assert.AreEqual(IdxKey.FromInteger(435), splitKey);
            Assert.AreEqual(43, leaf.CellsCount);
            Assert.AreEqual(45, newPage.CellsCount);
            Assert.AreEqual(IdxKey.FromInteger(435), newPage.GetCell(0).Key);
        }

        [TestMethod]
        public void SplitAtKey455Size128()
        {
            var leaf = PrepareSplitLeaf();
            var newPage = new IdxNode(null);
            newPage.PageNumber = 888;
            IdxCell addedCell = IdxCell.CreateLeafCell(IdxKey.FromInteger(455), new byte[116]);
            IdxKey splitKey = leaf.SplitLeaf(newPage, addedCell);
            Assert.AreEqual(IdxKey.FromInteger(440), splitKey);
            Assert.AreEqual(43, leaf.CellsCount);
            Assert.AreEqual(45, newPage.CellsCount);
            Assert.AreEqual(IdxKey.FromInteger(455), newPage.GetCell(2).Key);
        }

        private IdxNode PrepareSplitLeaf()
        {
            var cells = new List<IdxCell>();
            for (int i = 1; i <= 12; i++)
                cells.Add(CellForSplit(i, 116));
            for (int i = 1; i <= 64; i++)
                cells.Add(CellForSplit(i + 12, 4));
            for (int i = 1; i <= 11; i++)
                cells.Add(CellForSplit(i + 12 + 64, 116));
            var leaf = new IdxNode(null);
            foreach (var cell in cells)
                leaf.AddCell(cell);
            leaf.Save();
            return leaf;
        }

        private IdxCell CellForSplit(int key, int length)
        {
            var value = new byte[length];
            value[0] = (byte)key;
            return IdxCell.CreateLeafCell(IdxKey.FromInteger(10 * key), value);
        }
    }
}
