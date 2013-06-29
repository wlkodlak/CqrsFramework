using CqrsFramework.KeyValueStore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Tests.KeyValueStore
{
    [TestClass]
    public class KeyValueStoreTestMemory : KeyValueStoreTestBase
    {
        protected override IBuildStore CreateBuilder()
        {
            return new Builder();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
        private class Builder : IBuildStore
        {
            private List<KeyValueDocument> _initial = new List<KeyValueDocument>();
            private MemoryKeyValueStore _store = new MemoryKeyValueStore();

            public void With(KeyValueDocument document)
            {
                _store.SetupDocument(document);
            }

            public List<KeyValueDocument> GetAll()
            {
                return _store.GetAllDocuments();
            }

            public IKeyValueStore Build()
            {
                return _store;
            }
        }
    }
}
