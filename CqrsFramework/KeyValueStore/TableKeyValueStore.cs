using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.KeyValueStore
{
    public class TableKeyValueStore : IKeyValueStore
    {
        private ITableProvider _provider;

        public TableKeyValueStore(ITableProvider provider)
        {
            _provider = provider;
        }

        public KeyValueDocument Get(string key)
        {
            var row = _provider.GetRows().Where("key").Is(key).FirstOrDefault();
            return row == null ? null : new KeyValueDocument(row.Get<string>("key"), row.Get<int>("version"), row.Get<byte[]>("data"));
        }

        public int Set(string key, int expectedVersion, byte[] data)
        {
            var row = _provider.GetRows().Where("key").Is(key).FirstOrDefault();
            var version = row == null ? 0 : row.Get<int>("version");
            VerifyVersion(key, expectedVersion, version);
            if (row == null)
            {
                row = _provider.NewRow();
                row["key"] = key;
                row["version"] = 1;
                row["data"] = data;
                _provider.Insert(row);
                return 1;
            }
            else
            {
                row["version"] = version + 1;
                row["data"] = data;
                _provider.Update(row);
                return version + 1;
            }
        }

        private void VerifyVersion(string key, int expected, int actual)
        {
            if (expected != -1 && expected != actual)
                throw KeyValueStoreException.BadVersion(key, expected, actual);
        }

        public IEnumerable<string> Enumerate()
        {
            return _provider.GetRows().Select(row => row.Get<string>("key")).ToList();
        }

        public void Flush()
        {
        }

        public void Purge()
        {
            _provider.GetRows().ToList().ForEach(_provider.Delete);
        }

        public void Dispose()
        {
        }
    }
}
