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
        private bool _exists;
        private int _minKnownPosition = 1;
        private int _maxKnownPosition = 0;
        private List<long> _eventPositions = new List<long>();
        private Dictionary<int, FileEventStoreEntry> _cachedEvents = new Dictionary<int, FileEventStoreEntry>();
        private FileEventStoreEntry _cachedSnapshot;

        public FileEventStream(FileEventStore store, string name, bool exists)
        {
            _store = store;
            _name = name;
            _exists = exists;
        }

        public bool IsEmpty
        {
            get { return !_exists; }
        }

        public int GetCurrentVersion()
        {
            lock (_store.Lock())
            {
                LoadPositionsSince(_maxKnownPosition + 1);
                return _maxKnownPosition;
            }
        }

        private void LoadPositionsSince(int version)
        {
            version = Math.Max(1, Math.Min(_maxKnownPosition + 1, version));
            if (version == (_maxKnownPosition + 1))
            {
                var positions = _store.FindEvents(_name, _maxKnownPosition + 1).Select(p => p.Value).ToList();
                _eventPositions.AddRange(positions);
                _maxKnownPosition += positions.Count;
                _exists = _maxKnownPosition > 0;
            }
            else if (version < _minKnownPosition)
            {
                var positions = _store.FindEvents(_name, version).Select(p => p.Value).ToList();
                _minKnownPosition = version;
                _maxKnownPosition = version + positions.Count - 1;
                _exists = _maxKnownPosition > 0;
                _eventPositions.Clear();
                _eventPositions.AddRange(positions);
            }
        }

        public string GetName()
        {
            lock (_store.Lock())
            {
                return _name;
            }
        }

        public int GetSnapshotVersion()
        {
            lock (_store.Lock())
            {
                if (_cachedSnapshot != null)
                    return _cachedSnapshot.Version;
                return _store.FindSnapshot(_name).Key;
            }
        }

        public EventStoreSnapshot GetSnapshot()
        {
            lock (_store.Lock())
            {
                if (_cachedSnapshot != null)
                    return new EventStoreSnapshot { Version = _cachedSnapshot.Version, Key = _name, Data = _cachedSnapshot.Data };
                var pair = _store.FindSnapshot(_name);
                if (pair.Key == 0)
                    return null;
                _cachedSnapshot = _store.LoadSnapshotFrom(pair.Value);
                if (_cachedSnapshot != null)
                    return new EventStoreSnapshot { Version = _cachedSnapshot.Version, Key = _name, Data = _cachedSnapshot.Data };
                else
                    return null;
            }
        }

        public IEnumerable<EventStoreEvent> GetEvents(int minVersion)
        {
            lock (_store.Lock())
            {
                LoadPositionsSince(minVersion);

                int startVersion = Math.Max(minVersion, _minKnownPosition);
                int stopVersion = _maxKnownPosition;

                var list = new List<EventStoreEvent>();

                for (int version = startVersion; version <= stopVersion; version++)
                {
                    FileEventStoreEntry entry;
                    if (!_cachedEvents.TryGetValue(version, out entry))
                    {
                        var listIndex = version - _minKnownPosition;
                        entry = _store.LoadEventFrom(_eventPositions[listIndex]);
                        if (entry == null)
                            throw new EventStoreException(string.Format("Cannot load event {0}:{1}", _name, version));
                        _cachedEvents[version] = entry;
                    }
                    var @event = new EventStoreEvent
                    {
                        Key = _name,
                        Published = entry.Published,
                        Version = entry.Version,
                        Clock = entry.Clock,
                        Data = entry.Data
                    };
                    list.Add(@event);
                }

                return list;
            }
        }

        public void SaveEvents(int expectedVersion, EventStoreEvent[] events)
        {
            lock (_store.Lock())
            {
                LoadPositionsSince(_maxKnownPosition + 1);
                if (expectedVersion != -1 && _maxKnownPosition != expectedVersion)
                    throw EventStoreException.UnexpectedVersion(_name, expectedVersion, _maxKnownPosition);

                foreach (var @event in events)
                {
                    var entry = new FileEventStoreEntry();
                    entry.Data = @event.Data;
                    entry.IsEvent = true;
                    entry.Key = _name;
                    entry.Published = @event.Published;
                    entry.Version = @event.Version;
                    _store.SaveEvent(entry);
                    _cachedEvents.Add(entry.Version, entry);
                    _eventPositions.Add(entry.Position);
                    _maxKnownPosition++;
                    @event.Clock = entry.Clock;
                    _exists = true;
                }
            }
        }

        public void SaveSnapshot(EventStoreSnapshot snapshot)
        {
            lock (_store.Lock())
            {
                if (snapshot == null)
                    return;
                var lastSnapshotVersion = _store.FindSnapshot(_name).Key;
                if (snapshot.Version < lastSnapshotVersion)
                    return;
                var entry = new FileEventStoreEntry();
                entry.Data = snapshot.Data;
                entry.IsSnapshot = true;
                entry.Key = _name;
                entry.Version = snapshot.Version;
                _store.SaveSnapshot(entry);
                _cachedSnapshot = entry;
            }
        }
    }
}
