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
                    new IndexTableKeyValueStoreCompositeKey(document.Key, document.Version).IdxKey,
                    document.Data);
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
                    var composite = new IndexTableKeyValueStoreCompositeKey(pair.Key);
                    list.Add(new KeyValueDocument(composite.Key, composite.Version, pair.Value));
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
