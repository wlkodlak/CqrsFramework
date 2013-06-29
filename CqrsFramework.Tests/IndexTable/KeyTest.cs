using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.IndexTable
{
    [TestClass]
    public class KeyTest
    {
        [TestMethod]
        public void CreateKeyFromString()
        {
            AssertKeyBytes(IdxKey.FromString("Hello"), Encoding.ASCII.GetBytes("Hello"));
        }

        [TestMethod]
        public void CreateKeyFromInteger()
        {
            AssertKeyBytes(IdxKey.FromInteger(  0x48c32e), 0x80, 0x48, 0xc3, 0x2e);
            AssertKeyBytes(IdxKey.FromInteger(-0x3c29af0), 0x7c, 0x3d, 0x65, 0x10);
            AssertKeyBytes(IdxKey.FromInteger(int.MaxValue), 0xff, 0xff, 0xff, 0xff);
            AssertKeyBytes(IdxKey.FromInteger(int.MinValue), 0x00, 0x00, 0x00, 0x00);
        }

        [TestMethod]
        public void CreateKeyFromBytes()
        {
            var bytes = Encoding.ASCII.GetBytes("Trouble ahead !!!");
            AssertKeyBytes(IdxKey.FromBytes(bytes), bytes);
        }

        [TestMethod]
        public void EqualKeys()
        {
            var key1 = IdxKey.FromString("Hello");
            var key2 = IdxKey.FromString("Hello");
            var key3 = IdxKey.FromString("Help!");

            Assert.AreEqual(key1, key2);
            Assert.AreNotEqual(key1, key3);
            Assert.AreEqual(key1.GetHashCode(), key2.GetHashCode());
        }

        [TestMethod]
        public void ImplementsToString()
        {
            var key = IdxKey.FromBytes(new byte[] { 0x7f, 0xff, 0x03, 0xab, 0x44, 0xa3 });
            Assert.AreEqual("7fff 03ab 44a3", key.ToString());
        }

        [TestMethod]
        public void Compare()
        {
            var a = IdxKey.FromInteger(-4932);
            var b = IdxKey.FromInteger(214);
            var c = IdxKey.FromInteger(214);
            var d = IdxKey.FromInteger(7768);

            Assert.AreEqual(-1, IdxKey.Compare(a, b));
            Assert.AreEqual(1, IdxKey.Compare(b, a));
            Assert.AreEqual(0, IdxKey.Compare(b, c));
            Assert.AreEqual(0, IdxKey.Compare(c, b));
            Assert.AreEqual(-1, IdxKey.Compare(c, d));
            Assert.AreEqual(1, IdxKey.Compare(d, c));
        }

        [TestMethod]
        public void ComparisonOperators()
        {
            var a = IdxKey.FromInteger(-4932);
            var b = IdxKey.FromInteger(214);
            var c = IdxKey.FromInteger(214);
            var d = IdxKey.FromInteger(7768);

            Assert.IsFalse(a == b);
            Assert.IsTrue(a != b);
            Assert.IsTrue(b == c);
            Assert.IsFalse(b != c);

            Assert.IsTrue(a < b);
            Assert.IsFalse(a >= b);
            Assert.IsTrue(a <= b);
            Assert.IsFalse(a > b);
            Assert.IsTrue(b >= a);
            Assert.IsFalse(b < a);
            Assert.IsTrue(b > a);
            Assert.IsFalse(b <= a);

            Assert.IsFalse(b < c);
            Assert.IsTrue(b <= c);
            Assert.IsTrue(b >= c);
            Assert.IsFalse(b > c);

            Assert.IsTrue(c < d);
            Assert.IsFalse(c >= d);
            Assert.IsTrue(c <= d);
            Assert.IsFalse(c > d);
            Assert.IsTrue(d >= c);
            Assert.IsFalse(d < c);
            Assert.IsTrue(d > c);
            Assert.IsFalse(d <= c);
        }

        [TestMethod]
        public void MaxValue()
        {
            var a = IdxKey.MinValue;
            var b = IdxKey.FromString("ZZZZZZZ");
            var c = IdxKey.FromBytes(new byte[4] { 0xff, 0xff, 0xff, 0xff });
            var d = IdxKey.MaxValue;

            Assert.AreEqual(-1, IdxKey.Compare(a, b));
            Assert.AreEqual(-1, IdxKey.Compare(b, c));
            Assert.AreEqual(-1, IdxKey.Compare(c, d));
            Assert.AreEqual(1, IdxKey.Compare(b, a));
            Assert.AreEqual(1, IdxKey.Compare(c, b));
            Assert.AreEqual(1, IdxKey.Compare(d, c));
        }

        private void AssertKeyBytes(IdxKey key, params byte[] bytes)
        {
            CollectionAssert.AreEqual(bytes, key.ToBytes());
        }
    }
}
