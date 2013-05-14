using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class IndexTableLeafBasicTests
    {
        private byte[] RandomBytes(int seed, int length)
        {
            var bytes = new byte[length];
            new Random(seed).NextBytes(bytes);
            return bytes;
        }

        private void AssertEqualCells(IdxCell expected, IdxCell actual, string message)
        {
            Assert.AreEqual(expected.Key, actual.Key, string.Format("Key for {0}", message));
            Assert.AreEqual(expected.OverflowLength, actual.OverflowLength, string.Format("OverflowLength for {0}", message));
            Assert.AreEqual(expected.OverflowPage, actual.OverflowPage, string.Format("OverflowPage for {0}", message));
            CollectionAssert.AreEqual(
                expected.ValueBytes ?? new byte[0], 
                actual.ValueBytes ?? new byte[0], 
                string.Format("ValueBytes for {0}", message));
        }

        [TestMethod]
        public void EmptyNodeHasNoCells()
        {
            IdxLeaf node = new IdxLeaf((byte[])null);
            Assert.AreEqual(0, node.CellsCount);
            Assert.IsTrue(node.IsSmall, "Small");
            Assert.IsFalse(node.IsFull, "Full");
            Assert.IsFalse(node.IsDirty, "Dirty");
        }

        [TestMethod]
        public void NodeHasPageNumberAndNextLeafPointer()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.NextLeaf = 847;
            node.PageNumber = 447;
            Assert.AreEqual(847, node.NextLeaf, "Next leaf");
            Assert.AreEqual(447, node.PageNumber, "Page number");
        }

        [TestMethod]
        public void LeafCellCanBeAdded()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(5), RandomBytes(5471, 48)));
            Assert.AreEqual(1, node.CellsCount, "Cells count");
            Assert.AreEqual(IdxKey.FromInteger(5), node.GetCell(0).Key, "Cell 0 key");
            CollectionAssert.AreEqual(RandomBytes(5471, 48), node.GetCell(0).ValueBytes, "Cell value");
        }

        private int ChangedSizeAfterAdding(int cellSize, Func<IdxLeaf, bool> detectChange)
        {
            IdxLeaf node = new IdxLeaf(null);
            for (int i = 0; i < 1024; i++)
            {
                if (detectChange(node))
                    return node.CellsCount;
                else
                    node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), RandomBytes(847, cellSize - 12)));
            }
            return -1;
        }

        [TestMethod]
        public void EmptyCellChangesToNonSmallAfter63x16()
        {
            Assert.AreEqual(63, ChangedSizeAfterAdding(16, l => !l.IsSmall));
        }
        [TestMethod]
        public void EmptyCellChangesToNonSmallAfter8x128()
        {
            Assert.AreEqual(8, ChangedSizeAfterAdding(128, l => !l.IsSmall));
        }
        [TestMethod]
        public void EmptyCellChangesToFullAfter248x16()
        {
            Assert.AreEqual(248, ChangedSizeAfterAdding(16, l => l.IsFull));
        }
        [TestMethod]
        public void EmptyCellChangesToFullAfter31x128()
        {
            Assert.AreEqual(31, ChangedSizeAfterAdding(128, l => l.IsFull));
        }

        [TestMethod]
        public void DirtyFlagSetByAdding()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(44), null));
            Assert.IsTrue(node.IsDirty);
        }

        [TestMethod]
        public void DirtyFlagSetByRemoving()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(44), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(48), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(52), null));
            node.Save();
            node.RemoveCell(1);
            Assert.IsTrue(node.IsDirty);
        }

        [TestMethod]
        public void DirtyFlagSetByChangingNextPage()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.NextLeaf = 8475;
            Assert.IsTrue(node.IsDirty);
        }

        [TestMethod]
        public void SavingClearsDirtyFlag()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(44), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(48), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(52), null));
            Assert.IsTrue(node.IsDirty);
            node.Save();
            Assert.IsFalse(node.IsDirty);
        }

        [TestMethod]
        public void AddingCellsKeepsCellsSorted()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(15242), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(685), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(1234), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(24752), null));
            Assert.AreEqual(IdxKey.FromInteger(685), node.GetCell(0).Key, "Key for cell 0");
            Assert.AreEqual(IdxKey.FromInteger(1234), node.GetCell(1).Key, "Key for cell 1");
            Assert.AreEqual(IdxKey.FromInteger(15242), node.GetCell(2).Key, "Key for cell 2");
            Assert.AreEqual(IdxKey.FromInteger(24752), node.GetCell(3).Key, "Key for cell 3");
        }

        [TestMethod]
        public void CellsHaveOrdinalValue()
        {
            IdxLeaf node = new IdxLeaf(null);
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(15242), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(685), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(1234), null));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(24752), null));
            Assert.AreEqual(4, node.CellsCount);
            for (int i = 0; i < node.CellsCount; i++)
                Assert.AreEqual(i, node.GetCell(i).Ordinal, "Invalid ordinal for cell {0}.", i);
        }

        [TestMethod]
        public void RemovingEnoughCellsFromFullMakesItSmall()
        {
            IdxLeaf node = new IdxLeaf(null);
            for (int i = 0; i < 1024; i++)
                if (!node.IsFull)
                    node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(i), RandomBytes(847, 116)));
            for (int i = 0; i < 1024; i++)
                if (!node.IsSmall && node.CellsCount > 0)
                    node.RemoveCell(0);
            Assert.IsTrue(node.IsSmall, "Node should be small after removing");
            Assert.AreEqual(7, node.CellsCount, "Cells count");
            Assert.AreEqual(IdxKey.FromInteger(24), node.GetCell(0).Key, "First key after removing");
        }

        [TestMethod]
        public void LoadingLeafWith11Cells()
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(new byte[8] { 1, 11, 0, 0, 48, 2, 0, 0 });
                writer.Write(new byte[8]);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(547), RandomBytes(748, 8424)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(842), RandomBytes(84, 542)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(1047), RandomBytes(846, 16)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2074), RandomBytes(966, 2)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2099), RandomBytes(2471, 20)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2200), RandomBytes(2471, 847)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2202), RandomBytes(2471, 847)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2204), RandomBytes(2471, 847)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2208), RandomBytes(2471, 847)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2212), RandomBytes(2471, 847)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(2222), RandomBytes(2471, 847)).SaveLeafCell(writer);
            }
            IdxLeaf node = new IdxLeaf(buffer);
            Assert.AreEqual(11, node.CellsCount);
            Assert.AreEqual(560, node.NextLeaf);
            AssertEqualCells(IdxCell.CreateLeafCell(IdxKey.FromInteger(547), RandomBytes(748, 8424)), node.GetCell(0), "Cell 0");
            AssertEqualCells(IdxCell.CreateLeafCell(IdxKey.FromInteger(842), RandomBytes(84, 542)), node.GetCell(1), "Cell 1");
            AssertEqualCells(IdxCell.CreateLeafCell(IdxKey.FromInteger(1047), RandomBytes(846, 16)), node.GetCell(2), "Cell 2");
            AssertEqualCells(IdxCell.CreateLeafCell(IdxKey.FromInteger(2074), RandomBytes(966, 2)), node.GetCell(3), "Cell 3");
            AssertEqualCells(IdxCell.CreateLeafCell(IdxKey.FromInteger(2099), RandomBytes(2471, 20)), node.GetCell(4), "Cell 4");
        }

        [TestMethod]
        public void SavingLeafWith3Cells()
        {
            var buffer = new byte[PagedFile.PageSize];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(new byte[8] { 1, 3, 0, 0, 22, 0, 0, 0 });
                writer.Write(new byte[8]);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(547), RandomBytes(748, 8424)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(842), RandomBytes(84, 542)).SaveLeafCell(writer);
                IdxCell.CreateLeafCell(IdxKey.FromInteger(1047), RandomBytes(846, 16)).SaveLeafCell(writer);
            }

            IdxLeaf node = new IdxLeaf(null);
            node.NextLeaf = 22;
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(547), RandomBytes(748, 8424)));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(842), RandomBytes(84, 542)));
            node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromInteger(1047), RandomBytes(846, 16)));
            
            byte[] saved = node.Save();

            CollectionAssert.AreEqual(buffer, saved);
        }

        [TestMethod]
        public void SavingLeafWith31CellsOfSize128DoesNotCrash()
        {
            IdxLeaf node = new IdxLeaf(null);
            for (int i = 0; i < 1024; i++)
                if (!node.IsFull)
                    node.AddCell(IdxCell.CreateLeafCell(IdxKey.FromBytes(RandomBytes(i * 547, 120)), null));
            node.Save();
        }

        [TestMethod]
        public void LoadedAndSavedLeavesAreEqual()
        {
            IdxLeaf orig = new IdxLeaf(null);
            orig.NextLeaf = 8436;
            for (int i = 0; i < 64; i++)
                orig.AddCell(IdxCell.CreateLeafCell(IdxKey.FromBytes(RandomBytes(i * 547, 8)), null));
            for (int i = 0; i < 16; i++)
                orig.AddCell(IdxCell.CreateLeafCell(IdxKey.FromBytes(RandomBytes(i * 472, 56)), null));
            for (int i = 0; i < 8; i++)
                orig.AddCell(IdxCell.CreateLeafCell(IdxKey.FromBytes(RandomBytes(i * 998, 120)), null));
            
            var load = new IdxLeaf(orig.Save());

            Assert.AreEqual(orig.NextLeaf, load.NextLeaf);
            Assert.AreEqual(orig.CellsCount, load.CellsCount);
            for (int i = 0; i < orig.CellsCount; i++)
                AssertEqualCells(orig.GetCell(i), load.GetCell(i), string.Format("Cell {0}", i));
        }
    }
}
