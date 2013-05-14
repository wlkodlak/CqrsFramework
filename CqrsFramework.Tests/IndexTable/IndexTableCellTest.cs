using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableCellTest
    {
        [TestMethod]
        public void CreateLeafCellWithOverflow()
        {
            var cellData = new byte[482];
            for (int i = 0; i < 482; i++)
                cellData[i] = (byte)(i % 256);

            IdxCell cell = IdxCell.CreateLeafCell(IdxKey.FromInteger(3829), cellData);

            Assert.IsTrue(cell.IsLeaf);
            Assert.AreEqual(4, cell.KeyLength);
            Assert.AreEqual(IdxKey.FromInteger(3829), cell.Key);
            Assert.AreEqual(116, cell.ValueLength);
            Assert.AreEqual(1, cell.OverflowLength);
            Assert.AreEqual(0, cell.OverflowPage);
            Assert.AreEqual(128, cell.CellSize);
            CollectionAssert.AreEqual(cellData.Take(116).ToArray(), cell.ValueBytes);
        }

        [TestMethod]
        public void CreateLeafCellWithSmallValueAndStringKey()
        {
            var cellData = new byte[48];
            for (int i = 0; i < 48; i++)
                cellData[i] = (byte)i;

            IdxCell cell = IdxCell.CreateLeafCell(IdxKey.FromString("Hello World"), cellData);

            Assert.IsTrue(cell.IsLeaf);
            Assert.AreEqual(11, cell.KeyLength);
            Assert.AreEqual(IdxKey.FromString("Hello World"), cell.Key);
            Assert.AreEqual(48, cell.ValueLength);
            Assert.AreEqual(0, cell.OverflowLength);
            Assert.AreEqual(0, cell.OverflowPage);
            Assert.AreEqual(67, cell.CellSize);
            CollectionAssert.AreEqual(cellData, cell.ValueBytes);
        }

        [TestMethod]
        public void OverflowPageCanBeUpdatedAlways()
        {
            IdxCell cell = IdxCell.CreateLeafCell(IdxKey.FromString("Hello World"), null);
            cell.OverflowPage = 492;
            Assert.AreEqual(492, cell.OverflowPage);
        }

        [TestMethod]
        public void LimitKeyLength()
        {
            try
            {
                IdxCell.CreateLeafCell(IdxKey.FromBytes(new byte[130]), new byte[100]);
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        [TestMethod]
        public void LoadLeafCellFromBytes()
        {
            var keyBytes = Encoding.ASCII.GetBytes("Hello World");
            var valueBytes = new byte[48];
            new Random(493).NextBytes(valueBytes);
            var encodedCell = new List<byte>();
            encodedCell.AddRange(new byte[8] { 11, 48, 2, 0, 20, 1, 0, 0 });
            encodedCell.AddRange(keyBytes);
            encodedCell.AddRange(valueBytes);

            IdxCell cell;
            using (var reader = new BinaryReader(new MemoryStream(encodedCell.ToArray())))
                cell = IdxCell.LoadLeafCell(reader);

            Assert.IsTrue(cell.IsLeaf);
            Assert.AreEqual(11, cell.KeyLength);
            Assert.AreEqual(IdxKey.FromBytes(keyBytes), cell.Key);
            Assert.AreEqual(48, cell.ValueLength);
            CollectionAssert.AreEqual(valueBytes, cell.ValueBytes);
            Assert.AreEqual(2, cell.OverflowLength);
            Assert.AreEqual(276, cell.OverflowPage);
            Assert.AreEqual(encodedCell.Count, cell.CellSize);
        }

        [TestMethod]
        public void LoadTinyLeafCell()
        {
            var keyBytes = Encoding.ASCII.GetBytes("Hi");
            var valueBytes = new byte[2];
            new Random(77).NextBytes(valueBytes);
            var encodedCell = new List<byte>();
            encodedCell.AddRange(new byte[8] { 2, 2, 0, 0, 0, 0, 0, 0 });
            encodedCell.AddRange(keyBytes);
            encodedCell.AddRange(valueBytes);
            encodedCell.AddRange(new byte[4]);

            IdxCell cell;
            int endPosition;
            using (var reader = new BinaryReader(new MemoryStream(encodedCell.ToArray())))
            {
                cell = IdxCell.LoadLeafCell(reader);
                endPosition = (int)reader.BaseStream.Position;
            }

            Assert.IsTrue(cell.IsLeaf);
            Assert.AreEqual(2, cell.KeyLength);
            Assert.AreEqual(IdxKey.FromBytes(keyBytes), cell.Key);
            Assert.AreEqual(0, cell.OverflowLength);
            Assert.AreEqual(0, cell.OverflowPage);
            Assert.AreEqual(2, cell.ValueLength);
            CollectionAssert.AreEqual(valueBytes, cell.ValueBytes);
            Assert.AreEqual(16, cell.CellSize);
            Assert.AreEqual(16, endPosition);
        }

        [TestMethod]
        public void ExtremelySmallCellSize()
        {
            var minSize = PagedFile.PageSize / 256;
            var actual = IdxCell.CreateLeafCell(IdxKey.FromString("Hi"), null).CellSize;
            Assert.AreEqual(minSize, actual);
        }

        [TestMethod]
        public void SaveLeafCell()
        {
            var keyBytes = Encoding.ASCII.GetBytes("Hello World");
            var valueBytes = new byte[48];
            new Random(493).NextBytes(valueBytes);

            var cell = IdxCell.CreateLeafCell(IdxKey.FromBytes(keyBytes), valueBytes);
            cell.OverflowPage = 276;

            var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream))
                cell.SaveLeafCell(writer);

            var encodedCell = new List<byte>();
            encodedCell.AddRange(new byte[8] { 11, 48, 0, 0, 20, 1, 0, 0 });
            encodedCell.AddRange(keyBytes);
            encodedCell.AddRange(valueBytes);

            CollectionAssert.AreEqual(encodedCell.ToArray(), stream.ToArray());
        }

        [TestMethod]
        public void SaveTinyCellSaves16Bytes()
        {
            var cell = IdxCell.CreateLeafCell(IdxKey.FromString("A"), null);
            var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream))
                cell.SaveLeafCell(writer);
            Assert.AreEqual(16, stream.ToArray().Length);
        }

        [TestMethod]
        public void CreateInteriorCell()
        {
            IdxCell cell = IdxCell.CreateInteriorCell(IdxKey.FromInteger(53), 314);
            Assert.IsFalse(cell.IsLeaf);
            Assert.AreEqual(IdxKey.FromInteger(53), cell.Key);
            Assert.AreEqual(4, cell.KeyLength);
            Assert.AreEqual(314, cell.ChildPage);
            Assert.AreEqual(16, cell.CellSize);
        }

        [TestMethod]
        public void LoadInteriorCellFromBytes()
        {
            var keyBytes = Encoding.ASCII.GetBytes("Hello");
            var encodedCell = new List<byte>();
            encodedCell.AddRange(new byte[8] { 5, 0, 0, 0, 20, 2, 0, 0 });
            encodedCell.AddRange(keyBytes);
            encodedCell.AddRange(new byte[3]);

            IdxCell cell;
            int endPosition;
            using (var reader = new BinaryReader(new MemoryStream(encodedCell.ToArray())))
            {
                cell = IdxCell.LoadInteriorCell(reader);
                endPosition = (int)reader.BaseStream.Position;
            }

            Assert.IsFalse(cell.IsLeaf);
            Assert.AreEqual(5, cell.KeyLength);
            Assert.AreEqual(IdxKey.FromBytes(keyBytes), cell.Key);
            Assert.AreEqual(532, cell.ChildPage);
            Assert.AreEqual(16, cell.CellSize);
            Assert.AreEqual(16, endPosition);
        }

        [TestMethod]
        public void SaveInteriorCell()
        {
            var buffer = new byte[24];
            var cell = IdxCell.CreateInteriorCell(IdxKey.FromString("Help me, please!"), 514);
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
                cell.SaveInteriorCell(writer);
            var expected = new List<byte>();
            expected.AddRange(new byte[8] { 16, 0, 0, 0, 2, 2, 0, 0 });
            expected.AddRange(IdxKey.FromString("Help me, please!").ToBytes());
            CollectionAssert.AreEqual(expected.ToArray(), buffer);
        }

    }
}
