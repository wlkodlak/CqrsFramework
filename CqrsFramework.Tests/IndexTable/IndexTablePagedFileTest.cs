using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.InFile;

namespace CqrsFramework.Tests
{
    [TestClass]
    public class IndexTablePagedFileTest
    {
        [TestMethod]
        public void CanGetFileSize()
        {
            var stream = new MemoryStream();
            stream.SetLength(4096 * 16);
            using (PagedFile file = new PagedFile(stream))
            {
                Assert.AreEqual(16, file.GetSize());
            }
        }

        [TestMethod]
        public void CanSetFileSize()
        {
            var stream = new MemoryStream();
            stream.SetLength(4096 * 16);
            using (PagedFile file = new PagedFile(stream))
            {
                file.SetSize(24);
            }
            Assert.AreEqual(24 * 4096, stream.ToArray().Length);
        }

        [TestMethod]
        public void CanSetPageContents()
        {
            var newPageData = new byte[4096];
            for (int i = 0; i < 4096; i++)
                newPageData[i] = (byte)(i % 256);

            var stream = new MemoryStream();
            stream.SetLength(4096 * 16);
            using (PagedFile file = new PagedFile(stream))
            {
                file.SetPage(4, newPageData.ToArray());
            }

            var streamData = stream.ToArray();
            for (int i = 0; i < 4096; i++)
            {
                Assert.AreEqual(newPageData[i], streamData[i + 4096 * 4]);
            }
        }

        [TestMethod]
        public void CanGetPageContents()
        {
            var newPageData = new byte[4096];
            for (int i = 0; i < 4096; i++)
                newPageData[i] = (byte)((i % 256) ^ 0x7A);

            var stream = new MemoryStream();
            stream.SetLength(16 * 4096);
            stream.Seek(7 * 4096, SeekOrigin.Begin);
            stream.Write(newPageData, 0, 4096);
            stream.Seek(0, SeekOrigin.Begin);

            using (PagedFile file = new PagedFile(stream))
            {
                var page7 = file.GetPage(7);
                for (int i = 0; i < 4096; i++)
                {
                    Assert.AreEqual(newPageData[i], page7[i]);
                }
            }

        }

        [TestMethod]
        public void FailOnSettingOutOfRange()
        {
            try
            {
                var newPageData = new byte[4096];
                for (int i = 0; i < 4096; i++)
                    newPageData[i] = (byte)((i % 256) ^ 0x7A);

                var stream = new MemoryStream();
                stream.SetLength(16 * 4096);
                using (PagedFile file = new PagedFile(stream))
                    file.SetPage(24, newPageData);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        [TestMethod]
        public void FailOnGettingOutOfRange()
        {
            try
            {
                var stream = new MemoryStream();
                stream.SetLength(16 * 4096);
                using (PagedFile file = new PagedFile(stream))
                    file.GetPage(24);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        [TestMethod]
        public void FailWhenDataForSettingIsTooLarge()
        {
            try
            {
                var newPageData = new byte[5555];
                for (int i = 0; i < newPageData.Length; i++)
                    newPageData[i] = (byte)((i % 256) ^ 0x1A);

                var stream = new MemoryStream();
                stream.SetLength(16 * 4096);
                using (PagedFile file = new PagedFile(stream))
                    file.SetPage(7, newPageData);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }
    }
}
