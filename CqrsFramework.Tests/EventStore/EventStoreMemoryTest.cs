using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.InMemory;

namespace CqrsFramework.Tests
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

            public IEventStore Build()
            {
                return _store;
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                _store.SetupStream(name, snapshot, events);
            }
        }
    }
}
