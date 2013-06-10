using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    internal class FileEventStream : IEventStream
    {
        private FileEventStore _store;
        private string _name;
        private FileEventStoreEntry _snapshot;
        private List<FileEventStoreEntry> _events;

        public FileEventStream(FileEventStore store, string name)
        {
            _store = store;
            _name = name;
            _events = new List<FileEventStoreEntry>();
            IsEmpty = true;
        }

        public bool IsEmpty;

        public void MarkAsPublished(EventStoreEvent @event)
        {
            var entry = _events.FirstOrDefault(e => e.Version == @event.Version);
            if (entry != null)
            {
                @event.Published = true;
                entry.Published = true;
                _store.MarkPositionAsPublished(entry.Position);
            }
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
            if (_snapshot == null)
                return null;
            return new EventStoreSnapshot
            {
                Key = _name,
                Version = _snapshot.Version,
                Data = _snapshot.Data
            };
        }

        public IEnumerable<EventStoreEvent> GetEvents(int minVersion)
        {
            return _events
                .Where(e => e.Version >= minVersion)
                .Select(e => new EventStoreEvent { Key = e.Key, Version = e.Version, Published = e.Published, Data = e.Data })
                .ToList();
        }

        public void SaveEvents(int expectedVersion, EventStoreEvent[] events)
        {
            if (expectedVersion != -1 && _events.Count != expectedVersion)
                throw EventStoreException.UnexpectedVersion(_name, expectedVersion, _events.Count);

            foreach (var @event in events)
            {
                var entry = new FileEventStoreEntry();
                entry.Data = @event.Data;
                entry.IsEvent = true;
                entry.Key = _name;
                entry.Published = @event.Published;
                entry.Version = @event.Version;
                _store.AppendEntryToDataFile(entry);
                _events.Add(entry);
                _store.AppendEntryToUnpublished(entry);
            }
        }

        public void SaveSnapshot(EventStoreSnapshot snapshot)
        {
            if (_snapshot == null || _snapshot.Version < snapshot.Version)
            {
                var entry = new FileEventStoreEntry();
                entry.IsSnapshot = true;
                entry.Key = _name;
                entry.Data = snapshot.Data;
                entry.Version = snapshot.Version;
                _store.AppendEntryToDataFile(entry);
                _snapshot = entry;
            }
        }

        public void AppendEvent(FileEventStoreEntry entry)
        {
            _events.Add(entry);
            IsEmpty = false;
        }

        public void SetSnapshot(FileEventStoreEntry entry)
        {
            if (_snapshot == null || _snapshot.Version < entry.Version)
            {
                _snapshot = entry;
                IsEmpty = false;
            }
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            return _events
                .Where(e => e.Clock >= clock)
                .Select(e => new EventStoreEvent { Key = e.Key, Version = e.Version, Published = e.Published, Data = e.Data, Clock = e.Clock })
                .ToList();
        }

        public int GetSnapshotVersion()
        {
            return (_snapshot == null) ? 0 : _snapshot.Version;
        }
    }
}
