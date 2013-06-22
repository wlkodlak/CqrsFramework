using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.EventStore;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreMemoryTest : EventStoreTestBase
    {
        protected override IEventStoreTestBuilder CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IEventStoreTestBuilder
        {
            private MemoryEventStore _store = new MemoryEventStore();

            public IEventStore GetFull()
            {
                return _store;
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                _store.SetupStream(name, snapshot, events);
            }

            public void Dispose()
            {
                _store.Dispose();
            }

            public IEventStoreReader GetReader()
            {
                return _store;
            }

            public void Build()
            {
            }
        }
    }
}
