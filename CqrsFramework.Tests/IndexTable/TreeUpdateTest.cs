using System;
using System.Linq;
using System.Collections.Generic;
using Moq;
using CqrsFramework.IndexTable;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class TreeUpdateTest
    {
        [TestMethod]
        public void MediumToSmall()
        {
            var builder = new TestTreeBuilder(1024);
            {
                builder.SetNamedValue("original", builder.CreateValue(116));
                builder.SetNamedValue("updated", builder.CreateValue(20));
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(leaf, 1000);
                leaf.AddContents(28, builder.CreateValue(116), builder.LongCell(88, builder.GetNamedValue("original")), 92, builder.CreateValue(116));
                builder.Build();
            }

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Update(builder.BuildKey(88), builder.GetNamedValue("updated"));
            mock.Verify();

            {
                var cell = builder.GetNode(3).GetCell(1);
                CollectionAssert.AreEqual(builder.GetNamedValue("updated"), cell.ValueBytes);
            }
        }

        [TestMethod]
        public void SmallToGiant()
        {
            var builder = new TestTreeBuilder(1024);
            {
                builder.SetNamedValue("original", builder.CreateValue(20));
                builder.SetNamedValue("updated", builder.CreateValue(2857));
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(leaf, 1000);
                leaf.AddContents(28, builder.CreateValue(116), builder.LongCell(88, builder.GetNamedValue("original")), 92, builder.CreateValue(116));
                builder.Build();
            }

            var overflows = Enumerable.Range(0, 3).Select(i => builder.CreateOverflow()).ToArray();

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.SetupSequence(c => c.CreateOverflow(0)).Returns(overflows[0]).Returns(overflows[1]).Returns(overflows[2]);
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Update(builder.BuildKey(88), builder.GetNamedValue("updated"));
            mock.Verify();
            mock.Verify(c => c.CreateOverflow(0), Times.Exactly(3));

            {
                var cell = builder.GetNode(3).GetCell(1);
                Assert.AreEqual(116, cell.ValueLength);
                AssertLongValue(cell, 1024, builder.GetNamedValue("updated"), overflows);
            }
        }

        [TestMethod]
        public void GiantToBig()
        {
            var builder = new TestTreeBuilder(1024);
            {
                builder.SetNamedValue("original", builder.CreateValue(2857));
                builder.SetNamedValue("updated", builder.CreateValue(500));
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(leaf, 1000);
                leaf.AddContents(28, builder.CreateValue(116), builder.LongCell(88, builder.GetNamedValue("original"), 4, 5, 6), 92, builder.CreateValue(116));
                builder.Build();
            }

            var overflows = new IdxOverflow[1];
            overflows[0] = builder.GetOverflow(4);

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 4)).Returns(builder.GetOverflow(4)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 5)).Returns(builder.GetOverflow(5)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 6)).Returns(builder.GetOverflow(6)).Verifiable();
                mock.Setup(c => c.Delete(0, 6)).Verifiable();
                mock.Setup(c => c.Delete(0, 5)).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Update(builder.BuildKey(88), builder.GetNamedValue("updated"));
            mock.Verify();

            {
                var cell = builder.GetNode(3).GetCell(1);
                Assert.AreEqual(116, cell.ValueLength);
                AssertLongValue(cell, 1024, builder.GetNamedValue("updated"), overflows);
            }
        }

        [TestMethod]
        public void BigToGiant()
        {
            var builder = new TestTreeBuilder(1024);
            {
                builder.SetNamedValue("original", builder.CreateValue(500));
                builder.SetNamedValue("updated", builder.CreateValue(2857));
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(leaf, 1000);
                leaf.AddContents(28, builder.CreateValue(116), builder.LongCell(88, builder.GetNamedValue("original"), 4), 92, builder.CreateValue(116));
                builder.Build();
            }

            var overflows = new IdxOverflow[3];
            overflows[0] = builder.GetOverflow(4);
            overflows[1] = builder.CreateOverflow();
            overflows[2] = builder.CreateOverflow();

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.GetOverflow(0, 4)).Returns(builder.GetOverflow(4)).Verifiable();
                mock.SetupSequence(c => c.CreateOverflow(0)).Returns(overflows[1]).Returns(overflows[2]);
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Update(builder.BuildKey(88), builder.GetNamedValue("updated"));
            mock.Verify();
            mock.Verify(c => c.CreateOverflow(0), Times.Exactly(2));

            {
                var cell = builder.GetNode(3).GetCell(1);
                Assert.AreEqual(116, cell.ValueLength);
                AssertLongValue(cell, 1024, builder.GetNamedValue("updated"), overflows);
            }
        }

        [TestMethod]
        public void SmallToMediumWithoutFreeSpace()
        {
            var builder = new TestTreeBuilder(1024);
            {
                builder.SetNamedValue("original", builder.CreateValue(20));
                builder.SetNamedValue("updated", builder.CreateValue(116));
                var root = builder.Interior(2);
                var leaf = builder.Leaf(3);
                root.AddContents(leaf, 1000);
                leaf.AddContents(28, builder.CreateValue(116), builder.LongCell(88, builder.GetNamedValue("original")), 92, builder.CreateValue(56));
                leaf.AddContents(Enumerable.Range(12, 6).SelectMany(i => new object[] { i * 10, builder.CreateValue(116) }).ToArray());
                builder.Build();
            }

            var overflows = new IdxOverflow[1] { builder.CreateOverflow() };

            var mock = new Mock<IIdxContainer>(MockBehavior.Strict);
            {
                mock.Setup(c => c.GetPageSize()).Returns(1024);
                mock.Setup(c => c.WriteTree(0)).Returns(builder.GetNode(2)).Verifiable();
                mock.Setup(c => c.GetNode(0, 3)).Returns(builder.GetNode(3)).Verifiable();
                mock.Setup(c => c.CreateOverflow(0)).Returns(overflows[0]).Verifiable();
                mock.Setup(c => c.CommitWrite(0)).Verifiable();
            }

            var tree = new IdxTree(mock.Object, 0);
            tree.Update(builder.BuildKey(88), builder.GetNamedValue("updated"));
            mock.Verify();

            {
                var leaf = builder.GetNode(3);
                var cell = leaf.GetCell(1);
                Assert.IsTrue(20 <= cell.ValueLength && cell.ValueLength <= 48, "Expected value length 20-48, was {0}", cell.ValueLength);
                AssertLongValue(cell, 1024, builder.GetNamedValue("updated"), overflows);
                leaf.Save();
            }
        }



        private void AssertLongValue(IdxCell cell, int pageSize, byte[] expected, params IdxOverflow[] overflows)
        {
            Assert.AreEqual(overflows.Length, cell.OverflowLength, "expected overflow count");
            var buffer = new byte[cell.OverflowLength * IdxOverflow.Capacity(pageSize) + cell.ValueLength];
            Array.Copy(cell.ValueBytes, buffer, cell.ValueLength);
            if (overflows.Length > 0)
            {
                Assert.AreEqual(cell.OverflowPage, overflows[0].PageNumber, "Cell overflow page");
                for (int i = 1; i < overflows.Length; i++)
                    Assert.AreEqual(overflows[i].PageNumber, overflows[i - 1].Next, "Overflow {0} next page", i - 1);
                Assert.AreEqual(0, overflows[overflows.Length - 1].Next, "Last overflow next page");
                var offset = cell.ValueLength;
                for (int i = 0; i < overflows.Length; i++)
                    offset += overflows[i].ReadData(buffer, offset);
                Array.Resize(ref buffer, offset);
            }
            CollectionAssert.AreEqual(expected, buffer, "bytes");   
        }
    }
}
