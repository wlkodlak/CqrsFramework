using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.KeyValueStore
{
    [TestClass]
    public class KeyValueStoreIndexTableCompositeKeyTest
    {
        [TestMethod]
        public void CreateFromKeyAndVersion()
        {
            TestKeyFromKeyAndVersion("ZZZZZ", 0);
            TestKeyFromKeyAndVersion("Test.839", 8463572);
            TestKeyFromKeyAndVersion(new String('a', 600), 24423);
        }

        [TestMethod]
        public void CreateFromIndexKey()
        {
            TestKeyFromIndex("ZZZZZ", 0);
            TestKeyFromIndex("Test.839", 8463572);
            TestKeyFromIndex(new String('a', 600), 24423);
        }

        private void TestKeyFromKeyAndVersion(string key, int version)
        {
            var composite = new IndexTableKeyValueStoreCompositeKey(key, version);
            Assert.AreEqual(key, composite.Key, "Key");
            Assert.AreEqual(version, composite.Version, "Version");
            Assert.AreEqual(IdxKey.FromBytes(CreateComposite(key, version)), composite.IdxKey, "IdxKey ({0}/{1})", key, version);
        }

        private void TestKeyFromIndex(string key, int version)
        {
            var expected = IdxKey.FromBytes(CreateComposite(key, version));
            var composite = new IndexTableKeyValueStoreCompositeKey(expected);
            Assert.AreEqual(key, composite.Key, "Key");
            Assert.AreEqual(version, composite.Version, "Version");
            Assert.AreEqual(expected, composite.IdxKey, "IdxKey ({0}/{1})", key, version);
        }

        private byte[] CreateComposite(string key, int version)
        {
            var keyBytes = ByteArrayUtils.Utf8Text(key);
            var versionBytes = ByteArrayUtils.BinaryInt(version);
            return keyBytes.Concat(versionBytes).ToArray();
        }

    }
}
