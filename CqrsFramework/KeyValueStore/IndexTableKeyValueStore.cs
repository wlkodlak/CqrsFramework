﻿using CqrsFramework.IndexTable;
using CqrsFramework.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.KeyValueStore
{
    public class IndexTableKeyValueStore : IKeyValueStore
    {
        private IIdxContainer _container;
        private IIdxTree _tree;

        public IndexTableKeyValueStore(IIdxContainer container)
        {
            _container = container;
            _tree = new IdxTree(_container, 0);
        }

        public KeyValueDocument Get(string key)
        {
            var minKey = new IndexTableKeyValueStoreCompositeKey(key, 0).IdxKey;
            var maxKey = new IndexTableKeyValueStoreCompositeKey(key, int.MaxValue).IdxKey;
            return _tree.Select(minKey, maxKey).Select(CreateDocument).FirstOrDefault();
        }

        private static KeyValueDocument CreateDocument(KeyValuePair<IdxKey, byte[]> p)
        {
            var composite = new IndexTableKeyValueStoreCompositeKey(p.Key);
            return new KeyValueDocument(composite.Key, composite.Version, p.Value);
        }

        public int Set(string key, int expectedVersion, byte[] data)
        {
            var minKey = new IndexTableKeyValueStoreCompositeKey(key, 0).IdxKey;
            var maxKey = new IndexTableKeyValueStoreCompositeKey(key, int.MaxValue).IdxKey;
            var found = _tree.SelectKeys(minKey, maxKey).Take(1)
                .Select(p => new IndexTableKeyValueStoreCompositeKey(p)).FirstOrDefault();
            var foundVersion = found == null ? 0 : found.Version;
            if (expectedVersion != -1 && expectedVersion != foundVersion)
                throw KeyValueStoreException.BadVersion(key, expectedVersion, foundVersion);
            var finalKey = new IndexTableKeyValueStoreCompositeKey(key, foundVersion + 1);
            if (found != null)
                _tree.Delete(found.IdxKey);
            _tree.Insert(finalKey.IdxKey, data);
            return finalKey.Version;
        }

        public IEnumerable<string> Enumerate()
        {
            return _tree.SelectKeys(IdxKey.MinValue, IdxKey.MaxValue)
                .Select(p => new IndexTableKeyValueStoreCompositeKey(p).Key).ToList();
        }

        public void Flush()
        {
        }

        public void Purge()
        {
            _tree.Purge();
        }

        public void Dispose()
        {
            _container.Dispose();
        }

    }
}
