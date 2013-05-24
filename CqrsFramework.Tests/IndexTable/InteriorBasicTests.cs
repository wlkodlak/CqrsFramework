using System;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class InteriorBasicTests
    {
        [TestMethod]
        public void CreateEmptyNode()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.PageNumber = 394;
            Assert.IsInstanceOfType(node, typeof(IIdxNode), "Implements IIdxNode");
            Assert.IsFalse((node as IIdxNode).IsLeaf, "Interior node");
            Assert.AreEqual(394, node.PageNumber);
            Assert.AreEqual(0, node.CellsCount, "Cells count");
            Assert.AreEqual(0, node.LeftmostPage, "Leftmost page");
            Assert.IsTrue(node.IsSmall, "Small");
            Assert.IsFalse(node.IsFull, "Full");
            Assert.IsFalse(node.IsDirty, "Dirty");
        }

        [TestMethod]
        public void InteriorCellCanBeAdded()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.LeftmostPage = 744;
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(547), 995, 4096));
            Assert.AreEqual(1, node.CellsCount, "Cells count");
            Assert.AreEqual(744, node.LeftmostPage, "Leftmost page");
            Assert.IsNotNull(node.GetCell(0), "Cell 0");
            Assert.AreEqual(IdxKey.FromInteger(547), node.GetCell(0).Key, "Cell 0 key");
            Assert.AreEqual(995, node.GetCell(0).ChildPage, "Cell 0 page");
        }

        [TestMethod]
        public void PageNumber()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.PageNumber = 847;
            Assert.AreEqual(847, node.PageNumber);
        }

        private void EmptyPageChangesToAfterAdding(Func<IdxInterior, bool> predicate, int number, int keyLength)
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.LeftmostPage = 223;
            int reallyAdded = 0;
            for (int i = 0; i < 1024; i++)
            {
                if (predicate(node))
                {
                    reallyAdded = i;
                    break;
                }
                else
                {
                    var bytes = new byte[keyLength];
                    bytes[0] = (byte)(i / 256);
                    bytes[1] = (byte)(i % 256);
                    var page = i * 12;
                    node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromBytes(bytes), page, 4096));
                }
            }
            Assert.AreEqual(number, reallyAdded, "Number of added cells");
            Assert.AreEqual(reallyAdded, node.CellsCount, "Cells count");
        }

        [TestMethod]
        public void EmptyPageChangesToNonsmallAfterAdding63CellsOFKeySize4()
        {
            EmptyPageChangesToAfterAdding(n => !n.IsSmall, 63, 4);
        }

        [TestMethod]
        public void EmptyPageChangesToFullAfterAdding248CellsOFKeySize4()
        {
            EmptyPageChangesToAfterAdding(n => n.IsFull, 248, 4);
        }

        [TestMethod]
        public void EmptyPageChangesToNonsmallAfterAdding16CellsOFKeySize56()
        {
            EmptyPageChangesToAfterAdding(n => !n.IsSmall, 16, 56);
        }

        [TestMethod]
        public void EmptyPageChangesToFullAfterAdding31CellsOFKeySize120()
        {
            EmptyPageChangesToAfterAdding(n => n.IsFull, 31, 120);
        }

        [TestMethod]
        public void DirtyFlagIsSetByAddingCells()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            var dirty = new AssertDirtyChanged(node);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(5428), 96672, 4096));
            dirty.AssertTrue();
        }

        [TestMethod]
        public void DirtyFlagIsSetByRemovingCells()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            var dirty = new AssertDirtyChanged(node);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(5428), 96672, 4096));
            node.Save();
            dirty.AssertFalse("Not dirty before removing");
            node.RemoveCell(0);
            dirty.AssertTrue("Dirty after removing");
        }

        [TestMethod]
        public void DirtyFlagIsSetBySettingLeftmostPage()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            var dirty = new AssertDirtyChanged(node);
            node.LeftmostPage = 8754;
            dirty.AssertTrue();
        }

        [TestMethod]
        public void SavingClearsDirtyFlag()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            var dirty = new AssertDirtyChanged(node);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(5428), 96672, 4096));
            dirty.AssertTrue("Dirty before saving");
            node.Save();
            dirty.AssertFalse("Not dirty after saving");
        }

        [TestMethod]
        public void AddingCellsKeepsKeysOrdered()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(847), 111, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(-24), 222, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(952), 333, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(-84112), 444, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(9572), 333, 4096));
            Assert.AreEqual(IdxKey.FromInteger(-84112), node.GetCell(0).Key, "Cell 0 key");
            Assert.AreEqual(IdxKey.FromInteger(-24), node.GetCell(1).Key, "Cell 1 key");
            Assert.AreEqual(IdxKey.FromInteger(847), node.GetCell(2).Key, "Cell 2 key");
            Assert.AreEqual(IdxKey.FromInteger(952), node.GetCell(3).Key, "Cell 3 key");
            Assert.AreEqual(IdxKey.FromInteger(9572), node.GetCell(4).Key, "Cell 4 key");
        }

        [TestMethod]
        public void CellsHaveOrdinalValue()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(847), 111, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(-24), 222, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(952), 333, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(-84112), 444, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(9572), 333, 4096));
            for (int i = 0; i < 5; i++)
                Assert.AreEqual(i, node.GetCell(i).Ordinal, "Cell {0} ordinal", i);
        }

        [TestMethod]
        public void RemovingCell()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(2), 2, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(5), 5, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(8), 8, 4096));
            Assert.AreEqual(3, node.CellsCount, "There should be 3 cells before removing");
            Assert.AreEqual(2, node.GetCell(2).Ordinal, "Cells should have ordinal values before removing");
            node.RemoveCell(1);
            Assert.AreEqual(2, node.CellsCount, "Cells count");
            Assert.AreEqual(IdxKey.FromInteger(2), node.GetCell(0).Key, "Cell 0 key");
            Assert.AreEqual(IdxKey.FromInteger(8), node.GetCell(1).Key, "Cell 1 key");
            Assert.AreEqual(1, node.GetCell(1).Ordinal, "Cell 1 ordinal");
        }

        [TestMethod]
        public void Removing25CellsFromFullNodeOf128ByteCellsMakesItSmall()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            for (int i = 0; i < 1024; i++)
            {
                if (!node.IsFull)
                    node.AddCell(IdxCell.CreateInteriorCell(MakeLongKey(120, i), i, 4096));
            }
            Assert.IsTrue(node.IsFull, "Node should be full before removing");
            Assert.AreEqual(31, node.CellsCount, "Cells count before removing");
            for (int i = 0; i < 25; i++)
                node.RemoveCell(3);
            Assert.AreEqual(6, node.CellsCount, "Cells count");
            Assert.IsTrue(node.IsSmall, "Small");
            Assert.IsFalse(node.IsFull, "Full");
        }

        private static IdxKey MakeLongKey(int length, int key)
        {
            var bytes = new byte[120];
            bytes[2] = (byte)(key / 256);
            bytes[3] = (byte)(key % 256);
            return IdxKey.FromBytes(bytes);
        }

        [TestMethod]
        public void LoadingNode()
        {
            var buffer = new byte[4096];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(new byte[8] { 2, 3, 0, 0, 87, 100, 0, 0 });
                writer.Write(new byte[8]);
                IdxCell.CreateInteriorCell(IdxKey.FromString("Hello"), 8571, 4096).SaveInteriorCell(writer);
                IdxCell.CreateInteriorCell(IdxKey.FromString("Hi"), 741, 4096).SaveInteriorCell(writer);
                IdxCell.CreateInteriorCell(IdxKey.FromString("I was there"), 26837, 4096).SaveInteriorCell(writer);
            }
            IdxInterior node = new IdxInterior(buffer, 4096);
            Assert.AreEqual(3, node.CellsCount, "Cells count");
            Assert.AreEqual(25687, node.LeftmostPage, "Leftmost page");
            
            Assert.AreEqual(IdxKey.FromString("Hello"), node.GetCell(0).Key, "Cell 0 key");
            Assert.AreEqual(8571, node.GetCell(0).ChildPage, "Cell 0 page");
            Assert.AreEqual(0, node.GetCell(0).Ordinal, "Cell 0 ordinal");
            
            Assert.AreEqual(IdxKey.FromString("Hi"), node.GetCell(1).Key, "Cell 1 key");
            Assert.AreEqual(741, node.GetCell(1).ChildPage, "Cell 1 page");
            Assert.AreEqual(1, node.GetCell(1).Ordinal, "Cell 1 ordinal");

            Assert.AreEqual(IdxKey.FromString("I was there"), node.GetCell(2).Key, "Cell 2 key");
            Assert.AreEqual(26837, node.GetCell(2).ChildPage, "Cell 2 page");
            Assert.AreEqual(2, node.GetCell(2).Ordinal, "Cell 2ordinal");
        }

        [TestMethod]
        public void SavingNode()
        {
            var buffer = new byte[4096];
            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(new byte[8] { 2, 3, 0, 0, 87, 100, 0, 0 });
                writer.Write(new byte[8]);
                IdxCell.CreateInteriorCell(IdxKey.FromString("Hello"), 8571, 4096).SaveInteriorCell(writer);
                IdxCell.CreateInteriorCell(IdxKey.FromString("Hi"), 741, 4096).SaveInteriorCell(writer);
                IdxCell.CreateInteriorCell(IdxKey.FromString("I was there"), 26837, 4096).SaveInteriorCell(writer);
            }

            var node = new IdxInterior(null, 4096);
            node.LeftmostPage = 25687;
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromString("Hello"), 8571, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromString("Hi"), 741, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromString("I was there"), 26837, 4096));

            var saved = node.Save();

            CollectionAssert.AreEqual(buffer, saved);
        }

        [TestMethod]
        public void SavingFullNodeOf128LongCellsDoesNotCrash()
        {
            var node = new IdxInterior(null, 4096);
            var random = new Random(84752);
            for (int i = 0; i < 1024; i++)
            {
                if (!node.IsFull)
                {
                    var keyBytes = new byte[120];
                    random.NextBytes(keyBytes);
                    var cell = IdxCell.CreateInteriorCell(IdxKey.FromBytes(keyBytes), random.Next(), 4096);
                    node.AddCell(cell);
                }
            }
            Assert.IsTrue(node.IsFull, "Should be full before saving");
            node.Save();
        }

        [TestMethod]
        public void LoadingPreviouslySavedNodeEqualsOriginal()
        {
            var original = new IdxInterior(null, 4096);
            var random = new Random(84752);
            for (int i = 0; i < 1024; i++)
            {
                if (!original.IsFull)
                {
                    var keyBytes = new byte[120];
                    random.NextBytes(keyBytes);
                    var cell = IdxCell.CreateInteriorCell(IdxKey.FromBytes(keyBytes), random.Next(), 4096);
                    original.AddCell(cell);
                }
            }
            var loaded = new IdxInterior(original.Save(), 4096);
            Assert.AreEqual(original.CellsCount, loaded.CellsCount, "Cells count");
            Assert.AreEqual(original.LeftmostPage, loaded.LeftmostPage, "Leftmost page");
            Assert.AreEqual(original.IsSmall, loaded.IsSmall, "Small");
            Assert.AreEqual(original.IsFull, loaded.IsFull, "Full");
            for (int i = 0; i < original.CellsCount; i++)
            {
                var a = original.GetCell(i);
                var b = loaded.GetCell(i);
                Assert.AreEqual(a.Key, b.Key, "Cell {0} key", i);
                Assert.AreEqual(a.Ordinal, b.Ordinal, "Cell {0} ordinal", i);
                Assert.AreEqual(a.ChildPage, b.ChildPage, "Cell {0} page", i);
            }
        }

        [TestMethod]
        public void FindingChildPagesByKeyLeftmost()
        {
            IdxInterior node = new IdxInterior(null, 4096);
            node.LeftmostPage = 87;
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(2), 102, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(8), 88, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(20), 17, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(21), 192, 4096));
            node.AddCell(IdxCell.CreateInteriorCell(IdxKey.FromInteger(30), 332, 4096));

            Assert.AreEqual(87, node.FindPage(IdxKey.FromInteger(0)), "Key 0");
            Assert.AreEqual(332, node.FindPage(IdxKey.FromInteger(32)), "Key 32");
            Assert.AreEqual(17, node.FindPage(IdxKey.FromInteger(20)), "Key 20");
            Assert.AreEqual(192, node.FindPage(IdxKey.FromInteger(25)), "Key 25");
        }
    }
}
