using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.Tests.Infrastructure
{
    [TestClass]
    public class StreamProviderMemoryTest : StreamProviderTestBase
    {
        protected override IStreamProviderBuilder CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IStreamProviderBuilder
        {
            private MemoryStreamProvider _provider = new MemoryStreamProvider();

            public IStreamProvider Build()
            {
                return _provider;
            }

            public void AssertContents(string name, byte[] data)
            {
                var actual = _provider.GetContents(name);
                AssertExtension.AreEqual(data, actual);
            }

            public void PrepareStream(string name, byte[] buffer)
            {
                _provider.SetContents(name, buffer);
            }
        }
    }
}
