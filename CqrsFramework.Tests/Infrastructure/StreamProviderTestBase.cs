using System;
using System.IO;
using System.Text;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests
{
    public interface IStreamProviderBuilder
    {
        IStreamProvider Build();
        void AssertContents(string name, byte[] data);
        void PrepareStream(string name, byte[] buffer);
    }

    public abstract class StreamProviderTestBase
    {
        protected abstract IStreamProviderBuilder CreateBuilder();

        [TestMethod]
        public void CreateStream()
        {
            var builder = CreateBuilder();
            var buffer = Encoding.ASCII.GetBytes("Testing creating new stream");
            IStreamProvider provider = builder.Build();
            using (Stream stream = provider.Open("stream.5847", FileMode.Create))
                stream.Write(buffer, 0, buffer.Length);
            builder.AssertContents("stream.5847", buffer);
        }

        [TestMethod]
        public void ReadFromStream()
        {
            var builder = CreateBuilder();
            var originalContents = Encoding.ASCII.GetBytes("Testing reading from stream");
            builder.PrepareStream("file-84472", originalContents);
            IStreamProvider provider = builder.Build();
            var buffer = new byte[originalContents.Length + 1];
            using (Stream stream = provider.Open("file-84472", FileMode.Open))
            {
                var readCount = stream.Read(buffer, 0, buffer.Length);
                Assert.AreEqual(originalContents.Length, readCount);
                CollectionAssert.AreEqual(originalContents, buffer.Take(originalContents.Length).ToArray());
            }
        }

        [TestMethod]
        public void WriteInsideExisting()
        {
            var builder = CreateBuilder();
            var originalContents = Encoding.ASCII.GetBytes("Testing reading from stream");
            var expectedContents = Encoding.ASCII.GetBytes("Testing writing to a stream");
            builder.PrepareStream("test-847.25", originalContents);
            IStreamProvider provider = builder.Build();
            var buffer = new byte[1024];
            using (Stream stream = provider.Open("test-847.25", FileMode.Open))
            {
                var readCount = stream.Read(buffer, 0, buffer.Length);
                stream.Seek(8, SeekOrigin.Begin);
                var changeBuffer = Encoding.ASCII.GetBytes("writing to a");
                stream.Write(changeBuffer, 0, changeBuffer.Length);
            }
            builder.AssertContents("test-847.25", expectedContents);
        }

        [TestMethod]
        public void FailOnOpeningNonexistent()
        {
            try
            {
                var builder = CreateBuilder();
                builder.PrepareStream("test-847.28", null);
                IStreamProvider provider = builder.Build();
                using (Stream stream = provider.Open("test-847.28", FileMode.Open))
                    Assert.Fail("Expected IOException");
            }
            catch (IOException)
            {
            }
        }

        [TestMethod]
        public void FailOnCreatingExisting()
        {
            try
            {
                var builder = CreateBuilder();
                var originalContents = Encoding.ASCII.GetBytes("Testing reading from stream");
                builder.PrepareStream("test-847.30", originalContents);
                IStreamProvider provider = builder.Build();
                using (Stream stream = provider.Open("test-847.30", FileMode.CreateNew))
                    Assert.Fail("Expected IOException");
            }
            catch (IOException)
            {
            }
        }
    }
}
