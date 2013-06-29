using CqrsFramework.KeyValueStore;
using CqrsFramework.IndexTable;
using CqrsFramework.Infrastructure;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.KeyValueStore
{
    [TestClass]
    public class KeyValueStoreTestIndexTable : KeyValueStoreTestBase
    {
        protected override IBuildStore CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IBuildStore
        {
            private SharedMemoryStreamBuffer _buffer;
            private IIdxContainer _container;
            private IdxTree _tree;

            public Builder()
            {
                _buffer = new SharedMemoryStreamBuffer(0);
                _container = IdxContainer.OpenStream(new SharedMemoryStream(_buffer));
                _tree = new IdxTree(_container, 0);
            }

            public void With(KeyValueDocument document)
            {
                _tree.Insert(
                    IdxKey.FromBytes(ByteArrayUtils.Utf8Text(document.Key)),
                    ByteArrayUtils.BinaryInt(document.Version).Concat(document.Data).ToArray());
            }

            public List<KeyValueDocument> GetAll()
            {
                if (_tree == null)
                {
                    _container = IdxContainer.OpenStream(new SharedMemoryStream(_buffer));
                    _tree = new IdxTree(_container, 0);
                }
                var values = _tree.Select(IdxKey.MinValue, IdxKey.MaxValue);
                var list = new List<KeyValueDocument>();
                foreach (var pair in values)
                {
                    var key = ByteArrayUtils.Utf8Text(pair.Key.ToBytes());
                    var version = ByteArrayUtils.BinaryInt(pair.Value);
                    var data = pair.Value.Skip(4).ToArray();
                    list.Add(new KeyValueDocument(key, version, data));
                }
                return list;
            }

            public IKeyValueStore Build()
            {
                _tree = null;
                _container.Dispose();
                return new IndexTableKeyValueStore(IdxContainer.OpenStream(new SharedMemoryStream(_buffer)));
            }
        }
    }
}
