using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.Serialization
{
    [TestClass]
    public class ByteArrayUtilsTest
    {
        private struct IntegerTestCase
        {
            public int Number;
            public byte[] Bytes;
            public IntegerTestCase(int number, params byte[] bytes)
            {
                this.Number = number;
                this.Bytes = bytes;
            }
        }

        private struct LongTestCase
        {
            public long Number;
            public byte[] Bytes;
            public LongTestCase(long number, params byte[] bytes)
            {
                this.Number = number;
                this.Bytes = bytes;
            }
        }

        private struct ShortTestCase
        {
            public short Number;
            public byte[] Bytes;
            public ShortTestCase(short number, params byte[] bytes)
            {
                this.Number = number;
                this.Bytes = bytes;
            }
        }

        [TestMethod]
        public void BinaryInteger()
        {
            foreach (var item in new[] {
                new IntegerTestCase(-4724, 0xff, 0xff, 0xed, 0x8c),
                new IntegerTestCase(-1, 0xff, 0xff, 0xff, 0xff),
                new IntegerTestCase(int.MinValue, 0x80, 0x00, 0x00, 0x00),
                new IntegerTestCase(int.MaxValue, 0x7f, 0xff, 0xff, 0xff),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.BinaryInt(item.Number), string.Format("{0} to binary", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.BinaryInt(item.Bytes), string.Format("{0} from binary", item.Number));
            }
        }

        [TestMethod]
        public void BinaryLong()
        {
            foreach (var item in new[] {
                new LongTestCase(-4724, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xed, 0x8c),
                new LongTestCase(-1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
                new LongTestCase(long.MinValue, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00),
                new LongTestCase(long.MaxValue, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.BinaryLong(item.Number), string.Format("{0} to binary", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.BinaryLong(item.Bytes), string.Format("{0} from binary", item.Number));
            }
        }

        [TestMethod]
        public void BinaryShort()
        {
            foreach (var item in new[] {
                new ShortTestCase(-4724, 0xed, 0x8c),
                new ShortTestCase(-1, 0xff, 0xff),
                new ShortTestCase(short.MinValue, 0x80, 0x00),
                new ShortTestCase(short.MaxValue, 0x7f, 0xff),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.BinaryShort(item.Number), string.Format("{0} to binary", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.BinaryShort(item.Bytes), string.Format("{0} from binary", item.Number));
            }
        }

        [TestMethod]
        public void HexInteger()
        {
            foreach (var item in new[] {
                new IntegerTestCase(-4724, 0x46, 0x46, 0x46, 0x46, 0x45, 0x44, 0x38, 0x43),
                new IntegerTestCase(-1, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46),
                new IntegerTestCase(int.MinValue, 0x38, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30),
                new IntegerTestCase(int.MaxValue, 0x37, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.HexInt(item.Number), string.Format("{0} to hex", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.HexInt(item.Bytes), string.Format("{0} from hex", item.Number));
            }
        }

        [TestMethod]
        public void HexLong()
        {
            foreach (var item in new[] {
                new LongTestCase(-4724, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x45, 0x44, 0x38, 0x43),
                new LongTestCase(-1, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46),
                new LongTestCase(long.MinValue, 0x38, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30),
                new LongTestCase(long.MaxValue, 0x37, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46, 0x46),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.HexLong(item.Number), string.Format("{0} to hex", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.HexLong(item.Bytes), string.Format("{0} from hex", item.Number));
            }
        }

        [TestMethod]
        public void HexShort()
        {
            foreach (var item in new[] {
                new ShortTestCase(-4724, 0x45, 0x44, 0x38, 0x43),
                new ShortTestCase(-1, 0x46, 0x46, 0x46, 0x46),
                new ShortTestCase(short.MinValue, 0x38, 0x30, 0x30, 0x30),
                new ShortTestCase(short.MaxValue, 0x37, 0x46, 0x46, 0x46),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.HexShort(item.Number), string.Format("{0} to hex", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.HexShort(item.Bytes), string.Format("{0} from hex", item.Number));
            }
        }

        [TestMethod]
        public void TextInteger()
        {
            foreach (var item in new[] {
                new IntegerTestCase(-4724, 0x2d, 0x34, 0x37, 0x32, 0x34),
                new IntegerTestCase(-1, 0x2d, 0x31),
                new IntegerTestCase(int.MinValue, 0x2d, 0x32, 0x31, 0x34, 0x37, 0x34, 0x38, 0x33, 0x36, 0x34, 0x38),
                new IntegerTestCase(int.MaxValue, 0x32, 0x31, 0x34, 0x37, 0x34, 0x38, 0x33, 0x36, 0x34, 0x37),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.TextInt(item.Number), string.Format("{0} to text", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.TextInt(item.Bytes), string.Format("{0} from text", item.Number));
            }
        }

        [TestMethod]
        public void TextLong()
        {
            foreach (var item in new[] {
                new LongTestCase(-4724, 0x2d, 0x34, 0x37, 0x32, 0x34),
                new LongTestCase(-1, 0x2d, 0x31),
                new LongTestCase(long.MinValue, 0x2d, 0x39, 0x32, 0x32, 0x33, 0x33, 0x37, 0x32, 0x30, 0x33, 0x36, 0x38, 0x35, 0x34, 0x37, 0x37, 0x35, 0x38, 0x30, 0x38),
                new LongTestCase(long.MaxValue, 0x39, 0x32, 0x32, 0x33, 0x33, 0x37, 0x32, 0x30, 0x33, 0x36, 0x38, 0x35, 0x34, 0x37, 0x37, 0x35, 0x38, 0x30, 0x37),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.TextLong(item.Number), string.Format("{0} to text", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.TextLong(item.Bytes), string.Format("{0} from text", item.Number));
            }
        }

        [TestMethod]
        public void TextShort()
        {
            foreach (var item in new[] {
                new ShortTestCase(-4724, 0x2d, 0x34, 0x37, 0x32, 0x34),
                new ShortTestCase(-1, 0x2d, 0x31),
                new ShortTestCase(short.MinValue, 0x2d, 0x33, 0x32, 0x37, 0x36, 0x38),
                new ShortTestCase(short.MaxValue, 0x33, 0x32, 0x37, 0x36, 0x37),
            })
            {
                AssertExtension.AreEqual(item.Bytes, ByteArrayUtils.TextShort(item.Number), string.Format("{0} to text", item.Number));
                AssertExtension.AreEqual(item.Number, ByteArrayUtils.TextShort(item.Bytes), string.Format("{0} from text", item.Number));
            }
        }

        [TestMethod]
        public void Utf8Convert()
        {
            var expectedString = "Hello world!";
            var expectedBytes = new UTF8Encoding(false).GetBytes(expectedString);
            AssertExtension.AreEqual(expectedString, ByteArrayUtils.Utf8Text(expectedBytes));
            AssertExtension.AreEqual(expectedBytes, ByteArrayUtils.Utf8Text(expectedString));
        }
    }
}
