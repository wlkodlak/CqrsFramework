using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore.InMemory
{
    internal class MemoryEventStream : IEventStream
    {
        private MemoryEventStore _store;
        private string _name = null;
        private EventStoreSnapshot _snapshot = null;
        private List<EventStoreEvent> _events = new List<EventStoreEvent>();

        public MemoryEventStream(MemoryEventStore store, string name, EventStoreSnapshot snapshot, IEnumerable<EventStoreEvent> events)
        {
            _store = store;
            _name = name;
            _snapshot = snapshot;
            _events.AddRange(events);
        }

        public int GetCurrentVersion()
        {
            return _events.Count;
        }

        public string GetName()
        {
            return _name;
        }

        public EventStoreSnapshot GetSnapshot()
        {
            return _snapshot;
        }

        public IEnumerable<EventStoreEvent> GetEvents(int minVersion)
        {
            return _events.Where(e => e.Version >= minVersion);
        }

        public void SaveEvents(int expectedVersion, EventStoreEvent[] events)
        {
            var version = GetCurrentVersion();
            if (expectedVersion != -1 && version != expectedVersion)
                throw EventStoreException.UnexpectedVersion(_name, expectedVersion, version);
            _events.AddRange(events);
            _store.AddToUnpublished(events.Where(e => !e.Published));
        }

        public void SaveSnapshot(EventStoreSnapshot snapshot)
        {
            _snapshot = snapshot;
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            return _events.Where(e => e.Clock >= clock);
        }

        public int GetSnapshotVersion()
        {
            if (_snapshot == null)
                return 0;
            else
                return _snapshot.Version;
        }
    }
}
