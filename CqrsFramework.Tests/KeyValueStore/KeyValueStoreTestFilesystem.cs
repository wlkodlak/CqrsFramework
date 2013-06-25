using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Infrastructure;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
                _streams.SetContents(document.Key, GetVersionBytes(document.Version).Concat(document.Data).ToArray());
            }

            public List<KeyValueDocument> GetAll()
            {
                var list = new List<KeyValueDocument>();
                foreach (var name in _streams.GetStreams())
                {
                    var allBytes = _streams.GetContents(name);
                    var version = GetVersion(allBytes);
                    var contents = allBytes.Skip(4).ToArray();
                    list.Add(new KeyValueDocument(name, version, contents));
                }
                return list;
            }

            private int GetVersion(byte[] buffer)
            {
                var local = new byte[4];
                Array.Copy(buffer, local, 4);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(local);
                return BitConverter.ToInt32(local, 0);
            }

            private byte[] GetVersionBytes(int version)
            {
                var local = BitConverter.GetBytes(version);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(local);
                return local;
            }

            public IKeyValueStore Build()
            {
                return new FilesystemKeyValueStore(_streams);
            }
        }
    }
}
