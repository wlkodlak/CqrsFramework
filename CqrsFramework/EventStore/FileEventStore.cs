using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.EventStore
{
    public class FileEventStore : IEventStore
    {
        private FileEventStoreDataFile _dataFile;
        private Stream _indexFile;
        private Dictionary<string, FileEventStream> _streams;
        private List<FileEventStoreEntry> _unpublished;

        public FileEventStore(Stream dataFile, Stream indexFile)
        {
            _dataFile = new FileEventStoreDataFile(dataFile);
            _indexFile = indexFile;
            _streams = new Dictionary<string, FileEventStream>();
            _unpublished = new List<FileEventStoreEntry>();

            var entry = _dataFile.ReadEntry(0);
            while (entry != null)
            {
                FileEventStream stream;
                if (!_streams.TryGetValue(entry.Key, out stream))
                    _streams[entry.Key] = stream = new FileEventStream(this, entry.Key);
                if (entry.IsSnapshot)
                    stream.SetSnapshot(entry);
                else
                {
                    stream.AppendEvent(entry);
                    if (!entry.Published)
                        _unpublished.Add(entry);
                }
                entry = _dataFile.ReadEntry(entry.NextPosition);
            }
        }

        internal void MarkPositionAsPublished(long position)
        {
            _dataFile.MarkAsPublished(position);
        }

        public IEnumerable<EventStoreEvent> GetUnpublishedEvents()
        {
            return _unpublished
                .Where(entry => !entry.Published && entry.IsEvent)
                .Select(entry => new EventStoreEvent { Published = false, Key = entry.Key, Version = entry.Version, Data = entry.Data })
                .ToList();
        }

        public IEventStream GetStream(string name, EventStreamOpenMode mode)
        {
            FileEventStream stream;
            if (_streams.TryGetValue(name, out stream))
            {
                if (mode == EventStreamOpenMode.OpenExisting)
                    return stream;
                else if (stream.IsEmpty)
                    return stream;
                else
                    throw EventStoreException.StreamAlreadyExists(name);
            }
            else
            {
                if (mode == EventStreamOpenMode.OpenExisting)
                    throw EventStoreException.StreamDoesNotExist(name);
                else if (mode == EventStreamOpenMode.Open)
                    return null;
                else
                {
                    stream = new FileEventStream(this, name);
                    _streams[name] = stream;
                    return stream;
                }
            }
        }

        public void MarkAsPublished(EventStoreEvent @event)
        {
            FileEventStream stream;
            if (_streams.TryGetValue(@event.Key, out stream))
                stream.MarkAsPublished(@event);
        }

        public void Dispose()
        {
            _dataFile.Dispose();
            _indexFile.Dispose();
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            return _streams
                .SelectMany(s => s.Value.GetSince(clock))
                .OrderBy(e => e.Clock)
                .ToList();
        }

        internal void AppendEntryToDataFile(FileEventStoreEntry entry)
        {
            _dataFile.AppendEntry(entry);
        }

        internal void AppendEntryToUnpublished(FileEventStoreEntry entry)
        {
            _unpublished.Add(entry);
        }
    }
}
