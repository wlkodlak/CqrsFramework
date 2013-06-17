using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using CqrsFramework.IndexTable;

namespace CqrsFramework.EventStore
{
    public class FileEventStore : IEventStore
    {
        private FileEventStoreDataFile _dataFile;
        private IdxContainer _indexFile;
        private FileEventStoreIndexCore _indexCore;
        private Dictionary<long, FileEventStoreEntry> _unpublishedByPosition;
        private Dictionary<string, FileEventStoreEntry> _unpublishedByEvent;
        private Dictionary<string, FileEventStream> _streams;
        private long _clock = 0;
        private object _lock = new object();

        public FileEventStore(Stream dataFile, Stream indexFile)
        {
            _dataFile = new FileEventStoreDataFile(dataFile);
            _indexFile = IdxContainer.OpenStream(indexFile);
            _indexCore = new FileEventStoreIndexCore(new IdxTree(_indexFile, 0), new IdxTree(_indexFile, 1));
            _streams = new Dictionary<string, FileEventStream>();
            _unpublishedByPosition = new Dictionary<long, FileEventStoreEntry>();
            _unpublishedByEvent = new Dictionary<string, FileEventStoreEntry>(StringComparer.Ordinal);

            var entry = _dataFile.ReadEntry(_indexCore.UnpublishedPosition);
            if (entry == null)
                _dataFile.SetAppendPosition(_indexCore.AppendPosition);
            bool unpublishedEventFound = false;
            while (entry != null)
            {
                if (!entry.Published)
                {
                    _unpublishedByPosition.Add(entry.Position, entry);
                    _unpublishedByEvent.Add(UnpublishedKey(entry.Key, entry.Version), entry);
                    if (!unpublishedEventFound)
                    {
                        unpublishedEventFound = true;
                        _indexCore.UnpublishedPosition = entry.Position;
                    }
                }
                if (_clock < entry.Clock)
                    _clock = entry.Clock;
                entry = _dataFile.ReadEntry(entry.NextPosition);
            }
            _indexCore.AppendPosition = _clock;
            _indexCore.Flush();
        }

        private string UnpublishedKey(string name, int version)
        {
            return string.Format("{0}:{1}", name, version);
        }

        public IEnumerable<EventStoreEvent> GetUnpublishedEvents()
        {
            lock (_lock)
            {
                return _unpublishedByEvent.Values
                    .Where(entry => !entry.Published && entry.IsEvent)
                    .Select(entry => new EventStoreEvent { Published = false, Key = entry.Key, Version = entry.Version, Data = entry.Data })
                    .ToList();
            }
        }

        public IEventStream GetStream(string name, EventStreamOpenMode mode)
        {
            lock (_lock)
            {
                FileEventStream stream;
                if (_streams.TryGetValue(name, out stream))
                {
                    if (stream.IsEmpty)
                    {
                        if (mode == EventStreamOpenMode.Create)
                            return stream;
                        else if (mode == EventStreamOpenMode.Open)
                            return null;
                        else
                            throw EventStoreException.StreamDoesNotExist(name);
                    }
                    else
                    {
                        if (mode == EventStreamOpenMode.Create)
                            throw EventStoreException.StreamAlreadyExists(name);
                        else
                            return stream;
                    }
                }
                else
                {
                    if (_indexCore.StreamExists(name))
                    {
                        if (mode == EventStreamOpenMode.Create)
                            throw EventStoreException.StreamAlreadyExists(name);
                        else
                        {
                            stream = new FileEventStream(this, name, true);
                            _streams[name] = stream;
                            return stream;
                        }
                    }
                    else
                    {
                        if (mode == EventStreamOpenMode.Create)
                        {
                            stream = new FileEventStream(this, name, false);
                            _streams[name] = stream;
                            return stream;
                        }
                        else if (mode == EventStreamOpenMode.Open)
                            return null;
                        else
                            throw EventStoreException.StreamDoesNotExist(name);
                    }
                }
            }
        }

        public void MarkAsPublished(EventStoreEvent @event)
        {
            lock (_lock)
            {
                FileEventStoreEntry entry;
                var key = UnpublishedKey(@event.Key, @event.Version);
                if (!_unpublishedByEvent.TryGetValue(key, out entry))
                    return;
                entry.Published = true;
                @event.Published = true;
                _dataFile.MarkAsPublished(entry.Position);
                _unpublishedByEvent.Remove(key);
                _unpublishedByPosition.Remove(entry.Position);
            }
        }

        public void Dispose()
        {
            var firstUnpublished = _unpublishedByEvent.Values.OrderBy(entry => entry.Clock).FirstOrDefault(entry => !entry.Published && entry.IsEvent);
            _indexCore.AppendPosition = _clock;
            _indexCore.UnpublishedPosition = firstUnpublished == null ? _clock : firstUnpublished.Position;
            _indexCore.Flush();
            _dataFile.Dispose();
            _indexFile.Dispose();
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            var entry = _dataFile.ReadEntry(clock);
            if (entry == null)
                yield break;
            while (entry != null)
            {
                if (entry.IsEvent)
                {
                    yield return new EventStoreEvent
                    {
                        Published = entry.Published,
                        Key = entry.Key,
                        Version = entry.Version,
                        Data = entry.Data,
                        Clock = entry.Clock
                    };
                }
                entry = _dataFile.ReadEntry(entry.NextPosition);
            }
        }

        internal FileEventStoreEntry LoadSnapshotFrom(long position)
        {
            return _dataFile.ReadEntry(position);
        }

        internal FileEventStoreEntry LoadEventFrom(long position)
        {
            FileEventStoreEntry entry;
            if (_unpublishedByPosition.TryGetValue(position, out entry))
                return entry;
            else
                return _dataFile.ReadEntry(position);
        }

        internal void SaveEvent(FileEventStoreEntry entry)
        {
            _dataFile.AppendEntry(entry);
            _indexCore.AddEvent(entry.Key, entry.Version, entry.Position);
            if (_clock < entry.Clock)
                _clock = entry.Clock;
            if (!entry.Published)
            {
                _unpublishedByPosition.Add(entry.Position, entry);
                _unpublishedByEvent.Add(UnpublishedKey(entry.Key, entry.Version), entry);
            }
        }

        internal void SaveSnapshot(FileEventStoreEntry entry)
        {
            _dataFile.AppendEntry(entry);
            _indexCore.AddSnapshot(entry.Key, entry.Version, entry.Position);
            if (_clock < entry.Clock)
                _clock = entry.Clock;
        }

        internal object Lock()
        {
            return _lock;
        }

        internal IEnumerable<KeyValuePair<int, long>> FindEvents(string name, int version)
        {
            return _indexCore.FindEvents(name, version);
        }

        internal KeyValuePair<int, long> FindSnapshot(string name)
        {
            return _indexCore.FindSnapshot(name);
        }
    }
}
