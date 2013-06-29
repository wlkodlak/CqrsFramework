using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Infrastructure;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.KeyValueStore
{
    [TestClass]
    public class KeyValueStoreTestFilesystem : KeyValueStoreTestBase
    {
        protected override IBuildStore CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IBuildStore
        {
            private MemoryStreamProvider _streams = new MemoryStreamProvider();

            public void With(KeyValueDocument document)
            {
                var contents = ByteArrayUtils.HexInt(document.Version).Concat(new byte[2] { 0x0d, 0x0a }).Concat(document.Data).ToArray();
                _streams.SetContents(document.Key, contents);
            }

            public List<KeyValueDocument> GetAll()
            {
                var list = new List<KeyValueDocument>();
                foreach (var name in _streams.GetStreams())
                {
                    var allBytes = _streams.GetContents(name);
                    var version = ByteArrayUtils.HexInt(allBytes);
                    var contents = allBytes.Skip(10).ToArray();
                    list.Add(new KeyValueDocument(name, version, contents));
                }
                return list;
            }

            public IKeyValueStore Build()
            {
                return new FilesystemKeyValueStore(_streams);
            }
        }
    }
}
