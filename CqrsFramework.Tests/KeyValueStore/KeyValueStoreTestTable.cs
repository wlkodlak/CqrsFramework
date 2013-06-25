using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Infrastructure;
using System.Data;

namespace CqrsFramework.Tests.KeyValueStore
{
    [TestClass]
    public class KeyValueStoreTestTable : KeyValueStoreTestBase
    {
        protected override IBuildStore CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IBuildStore
        {
            private DataTable _table;
            private MemoryTableProvider _provider;

            public Builder()
            {
                _table = new DataTable("KeyValueStore");
                _table.Columns.Add("id", typeof(int));
                _table.Columns.Add("key", typeof(string));
                _table.Columns.Add("version", typeof(int));
                _table.Columns.Add("data", typeof(byte[]));
                _provider = new MemoryTableProvider(_table, null);
            }

            public void With(KeyValueDocument document)
            {
                var row = _provider.NewRow();
                row["key"] = document.Key;
                row["version"] = document.Version;
                row["data"] = document.Data;
                _provider.Insert(row);
            }

            public List<KeyValueDocument> GetAll()
            {
                return _provider.GetRows().Select(row => new KeyValueDocument(row.Get<string>("key"), row.Get<int>("version"), row.Get<byte[]>("data"))).ToList();
            }

            public IKeyValueStore Build()
            {
                return new TableKeyValueStore(_provider);
            }
        }
    }
}
