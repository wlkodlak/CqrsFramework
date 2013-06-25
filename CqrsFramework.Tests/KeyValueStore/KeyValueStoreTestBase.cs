using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.KeyValueStore;

namespace CqrsFramework.Tests.KeyValueStore
{
    public abstract class KeyValueStoreTestBase
    {
        public interface IBuildStore
        {
            void With(KeyValueDocument document);
            List<KeyValueDocument> GetAll();
            IKeyValueStore Build();
        }

        protected abstract IBuildStore CreateBuilder();

        private IBuildStore _builder;
        private Encoding _encoding;
        private List<KeyValueDocument> _defaultDocs;

        [TestInitialize]
        public void Initialize()
        {
            _builder = CreateBuilder();
            _encoding = new UTF8Encoding(false);
            _defaultDocs = new List<KeyValueDocument>();
        }

        private void AddDocument(string name, int version, string contents)
        {
            var doc = new KeyValueDocument(name, version, _encoding.GetBytes(contents));
            _defaultDocs.Add(doc);
            _builder.With(doc);
        }

        private void SetupDefaultDocuments()
        {
            AddDocument("doc1", 1, "Hello Doc 1");
            AddDocument("doc2", 3, "Hello Doc 2");
            AddDocument("doc3", 2, "Hello Doc 3");
            AddDocument("doc4", 1, "Hello Doc 4");
        }

        [TestMethod]
        public void EmptyStoreEnumeratesToNothing()
        {
            using (var store = _builder.Build())
                Assert.AreEqual(0, store.Enumerate().Count());
        }

        [TestMethod]
        public void StoreWithKeysEnumeratesToThose()
        {
            SetupDefaultDocuments();
            using (var store = _builder.Build())
            {
                var expected = new string[] { "doc1", "doc2", "doc3", "doc4" };
                var actual = store.Enumerate().ToArray();
                AssertExtension.AreEqual(expected, actual);
            }
        }

        [TestMethod]
        public void GetNonexistent()
        {
            SetupDefaultDocuments();
            using (var store = _builder.Build())
                Assert.IsNull(store.Get("doc0"));
        }

        [TestMethod]
        public void GetExisting()
        {
            SetupDefaultDocuments();
            using (var store = _builder.Build())
            {
                var found = store.Get("doc3");
                Assert.IsNotNull(found, "Not found");
                Assert.AreEqual("doc3", found.Key, "Key");
                Assert.AreEqual(2, found.Version, "Version");
                AssertExtension.AreEqual(_defaultDocs[2].Data, found.Data, "Data");
            }
        }

        [TestMethod]
        public void SetNewWithoutVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("New document");
            using (var store = _builder.Build())
            {
                var storedVersion = store.Set("doc9", -1, bytes);
                store.Flush();
                var storedDocument = _builder.GetAll().Where(a => !_defaultDocs.Any(d => a.Key == d.Key)).FirstOrDefault();
                Assert.AreEqual(1, storedVersion, "Returned version");
                Assert.IsNotNull(storedDocument, "Created");
                Assert.AreEqual("doc9", storedDocument.Key, "Key");
                Assert.AreEqual(1, storedDocument.Version, "Version");
                AssertExtension.AreEqual(bytes, storedDocument.Data, "Data");
            }
        }

        [TestMethod]
        public void SetNewWithVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("New document");
            using (var store = _builder.Build())
            {
                var storedVersion = store.Set("doc9", 0, bytes);
                store.Flush();
                var storedDocument = _builder.GetAll().Where(a => !_defaultDocs.Any(d => a.Key == d.Key)).FirstOrDefault();
                Assert.AreEqual(1, storedVersion, "Returned version");
                Assert.IsNotNull(storedDocument, "Created");
                Assert.AreEqual("doc9", storedDocument.Key, "Key");
                Assert.AreEqual(1, storedDocument.Version, "Version");
                AssertExtension.AreEqual(bytes, storedDocument.Data, "Data");
            }
        }

        [TestMethod, ExpectedException(typeof(KeyValueStoreException))]
        public void SetNewWithBadVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("New document");
            using (var store = _builder.Build())
                store.Set("doc9", 1, bytes);
        }

        [TestMethod]
        public void SetExistingWithoutVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("Updated document");
            int storedVersion;
            using (var store = _builder.Build())
                storedVersion = store.Set("doc2", -1, bytes);
            var modifiedDoc = _builder.GetAll().FirstOrDefault(d => d.Key == "doc2");
            Assert.AreEqual(4, modifiedDoc.Version, "Version");
            AssertExtension.AreEqual(bytes, modifiedDoc.Data, "Data");
        }

        [TestMethod]
        public void SetExistingWithVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("Updated document");
            int storedVersion;
            using (var store = _builder.Build())
                storedVersion = store.Set("doc2", 3, bytes);
            var modifiedDoc = _builder.GetAll().FirstOrDefault(d => d.Key == "doc2");
            Assert.AreEqual(4, modifiedDoc.Version, "Version");
            AssertExtension.AreEqual(bytes, modifiedDoc.Data, "Data");
        }

        [TestMethod, ExpectedException(typeof(KeyValueStoreException))]
        public void SetExistingWithBadVersion()
        {
            SetupDefaultDocuments();
            var bytes = _encoding.GetBytes("Updated document");
            int storedVersion;
            using (var store = _builder.Build())
                storedVersion = store.Set("doc2", 2, bytes);
        }

        [TestMethod]
        public void Purge()
        {
            SetupDefaultDocuments();
            using (var store = _builder.Build())
            {
                store.Purge();
                var keys = store.Enumerate().ToList();
                Assert.AreEqual(0, keys.Count, "Reported");
                Assert.AreEqual(0, _builder.GetAll().Count, "Real");
            }
        }

        [TestMethod]
        public void GetModified()
        {
            SetupDefaultDocuments();
            using (var store = _builder.Build())
            {
                var bytes = _encoding.GetBytes("Modified Doc2");
                var retVersion = store.Set("doc3", -1, bytes);
                var list = store.Enumerate().ToArray();
                var got = store.Get("doc3");
                Assert.AreEqual(3, retVersion, "Returned version");
                var expectedKeys = new string[] { "doc1", "doc2", "doc3", "doc4" };
                AssertExtension.AreEqual(expectedKeys, list, "Keys");
                Assert.AreEqual(3, got.Version, "Version");
                AssertExtension.AreEqual(bytes, got.Data, "Data");
            }
        }
    }
}
