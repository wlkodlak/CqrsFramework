using CqrsFramework.IndexTable;
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
            var idxKey = IdxKey.FromBytes(ByteArrayUtils.Utf8Text(key));
            return _tree.Select(idxKey, idxKey).Select(CreateDocument).FirstOrDefault();
        }

        private static KeyValueDocument CreateDocument(KeyValuePair<IdxKey, byte[]> p)
        {
            var key = ByteArrayUtils.Utf8Text(p.Key.ToBytes());
            var version = ByteArrayUtils.BinaryInt(p.Value);
            var data = p.Value.Skip(4).ToArray();
            return new KeyValueDocument(key, version, data);
        }

        public int Set(string key, int expectedVersion, byte[] data)
        {
            var idxKey = IdxKey.FromBytes(ByteArrayUtils.Utf8Text(key));
            var found = _tree.Select(idxKey, idxKey).Take(1).Select(CreateDocument).FirstOrDefault();
            var foundVersion = found == null ? 0 : found.Version;
            if (expectedVersion != -1 && expectedVersion != foundVersion)
                throw KeyValueStoreException.BadVersion(key, expectedVersion, foundVersion);
            var newContents = ByteArrayUtils.BinaryInt(foundVersion + 1).Concat(data).ToArray();
            if (found == null)
                _tree.Insert(idxKey, newContents);
            else
                _tree.Update(idxKey, newContents);
            return foundVersion + 1;
        }

        public IEnumerable<string> Enumerate()
        {
            return _tree.Select(IdxKey.MinValue, IdxKey.MaxValue).Select(p => ByteArrayUtils.Utf8Text(p.Key.ToBytes())).ToList();
        }

        public void Flush()
        {
        }

        public void Purge()
        {
            var keys = _tree.Select(IdxKey.MinValue, IdxKey.MaxValue).Select(p => p.Key).ToList();
            foreach (var key in keys)
                _tree.Delete(key);
        }

        public void Dispose()
        {
            _container.Dispose();
        }

    }
}
