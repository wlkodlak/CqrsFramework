using CqrsFramework.IndexTable;
using CqrsFramework.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.KeyValueStore
{
    public class IndexTableKeyValueStoreCompositeKey
    {
        public IndexTableKeyValueStoreCompositeKey(string key, int version)
        {
            this.Key = key;
            this.Version = version;
            var keyBytes = ByteArrayUtils.Utf8Text(key);
            var versionBytes = ByteArrayUtils.BinaryInt(version);
            var finalKey = new byte[keyBytes.Length + versionBytes.Length];
            Array.Copy(keyBytes, finalKey, keyBytes.Length);
            Array.Copy(versionBytes, 0, finalKey, keyBytes.Length, versionBytes.Length);
            this.IdxKey = IdxKey.FromBytes(finalKey);
        }

        public IndexTableKeyValueStoreCompositeKey(IdxKey idxKey)
        {
            this.IdxKey = idxKey;
            var bytes = IdxKey.ToBytes();
            this.Key = ByteArrayUtils.Utf8Text(bytes.Take(bytes.Length - 4).ToArray());
            this.Version = ByteArrayUtils.BinaryInt(bytes.Skip(bytes.Length - 4).ToArray());
        }

        public string Key { get; private set; }
        public int Version { get; private set; }
        public IdxKey IdxKey { get; private set; }
    }
}
