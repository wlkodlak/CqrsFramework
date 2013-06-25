using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.KeyValueStore
{
    public class MemoryKeyValueStore : IKeyValueStore
    {
        private Dictionary<string, KeyValueDocument> _documents = new Dictionary<string, KeyValueDocument>();

        public KeyValueDocument Get(string key)
        {
            KeyValueDocument found;
            _documents.TryGetValue(key, out found);
            return found;
        }

        public int Set(string key, int expectedVersion, byte[] data)
        {
            KeyValueDocument foundDocument;
            int foundVersion;
            if (!_documents.TryGetValue(key, out foundDocument))
                foundVersion = 0;
            else
                foundVersion = foundDocument.Version;

            if (expectedVersion != -1 && expectedVersion != foundVersion)
                throw KeyValueStoreException.BadVersion(key, expectedVersion, foundVersion);
            var newVersion = foundVersion + 1;
            _documents[key] = foundDocument = new KeyValueDocument(key, newVersion, data);
            return newVersion;
        }

        public IEnumerable<string> Enumerate()
        {
            return _documents.Keys;
        }

        public void Flush()
        {
        }

        public void Purge()
        {
            _documents.Clear();
        }

        public void Dispose()
        {
        }

        public void SetupDocument(KeyValueDocument document)
        {
            _documents[document.Key] = document;
        }

        public List<KeyValueDocument> GetAllDocuments()
        {
            return _documents.Values.ToList();
        }
    }
}
