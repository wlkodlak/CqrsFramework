using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.InTable
{
    public class TableEventStream : IEventStream
    {
        private string _name;
        private int _version;
        private int _snapshotVersion;
        private byte[] _snapshotData;
        private ITableProvider _tableStreams;
        private ITableProvider _tableEvents;
        private ITableProvider _tableSnapshots;

        internal static IEventStream Create(string name, EventStreamOpenMode mode, 
            ITableProvider tableStreams, ITableProvider tableEvents, ITableProvider tableSnapshots)
        {
            var stream = FindStream(name, tableStreams);

            if (stream == null)
            {
                if (mode == EventStreamOpenMode.Open)
                    return null;
                else if (mode == EventStreamOpenMode.OpenExisting)
                    throw EventStoreException.StreamDoesNotExist(name);
                else
                    return new TableEventStream
                    {
                        _name = name,
                        _snapshotVersion = 0,
                        _version = 0,
                        _tableStreams = tableStreams,
                        _tableEvents = tableEvents,
                        _tableSnapshots = tableSnapshots
                    };
            }
            else
            {
                stream._tableStreams = tableStreams;
                stream._tableEvents = tableEvents;
                stream._tableSnapshots = tableSnapshots;
                if (mode == EventStreamOpenMode.Open)
                    return stream;
                else if (mode == EventStreamOpenMode.OpenExisting)
                    return stream;
                else if (mode == EventStreamOpenMode.Create && stream._version == 0)
                    return stream;
                else
                    throw EventStoreException.StreamAlreadyExists(name);
            }
        }

        private static TableEventStream FindStream(string name, ITableProvider tableStreams)
        {
            var row = tableStreams.GetRows().Where("name").Is(name).SingleOrDefault();
            if (row == null)
                return null;
            var stream = new TableEventStream();
            stream._name = name;
            stream._version = row.Get<int>("version");
            stream._snapshotVersion = row.Get<int>("snapshotversion");
            return stream;
        }

        private TableEventStream()
        {
        }

        public int GetCurrentVersion()
        {
            return _version;
        }

        public string GetName()
        {
            return _name;
        }

        public EventStoreSnapshot GetSnapshot()
        {
            if (_snapshotVersion == 0)
                return null;
            else if (_snapshotData != null)
                return new EventStoreSnapshot { Key = _name, Version = _snapshotVersion, Data = _snapshotData };
            else
            {
                var row = _tableSnapshots.GetRows().Where("name").Is(_name).SingleOrDefault();
                if (row == null)
                {
                    _snapshotVersion = 0;
                    return null;
                }
                else
                {
                    _snapshotData = row.Get<byte[]>("snapshot");
                    return new EventStoreSnapshot { Key = _name, Version = _snapshotVersion, Data = _snapshotData };
                }
            }
        }

        public IEnumerable<EventStoreEvent> GetEvents(int minVersion)
        {
            var events = new List<EventStoreEvent>();
            var filterable = _tableEvents.GetRows();
            filterable = filterable.Where("name").Is(_name).And("version").IsAtLeast(minVersion);
            var table = filterable.OrderBy(r => r.Get<int>("version")).ToList();
            foreach (var item in table)
            {
                var @event = new EventStoreEvent();
                @event.Clock = item.Get<int>("clock");
                @event.Data = item.Get<byte[]>("data");
                @event.Key = item.Get<string>("name");
                @event.Published = item.Get<int>("published") == 1;
                @event.Version = item.Get<int>("version");
                events.Add(@event);
            }
            return events;
        }

        public void SaveEvents(int expectedVersion, EventStoreEvent[] events)
        {
            if (expectedVersion >= 0 && _version != expectedVersion)
                throw EventStoreException.UnexpectedVersion(_name, expectedVersion, _version);
            if (events.Length == 0)
                return;
            bool createStream = _version == 0;
            _version = events.Max(e => e.Version);
            SaveStreamToDb(createStream);
            foreach (var @event in events)
                SaveEventToDb(@event);
        }

        private void SaveEventToDb(EventStoreEvent @event)
        {
            var row = _tableEvents.NewRow();
            row["name"] = @event.Key;
            row["version"] = @event.Version;
            row["clock"] = (int)@event.Clock;
            row["published"] = @event.Published ? 1 : 0;
            row["data"] = @event.Data;
            _tableEvents.Insert(row);
        }

        private void SaveStreamToDb(bool createStream)
        {
            TableProviderRow row;
            if (createStream)
            {
                row = _tableStreams.NewRow();
                row["name"] = _name;
            }
            else
            {
                row = _tableStreams.GetRows().Where("name").Is(_name).Single();
            }
            row["version"] = _version;
            row["snapshotversion"] = _snapshotVersion;
            if (createStream)
            {
                _tableStreams.Insert(row);
            }
            else
                _tableStreams.Update(row);
        }

        private void SaveSnapshotToDb(EventStoreSnapshot snapshot, bool create)
        {
            TableProviderRow row = create ? null : _tableSnapshots.GetRows().Where("name").Is(_name).SingleOrDefault();
            if (row == null)
            {
                create = true;
                row = _tableSnapshots.NewRow();
                row["name"] = _name;
            }
            row["snapshot"] = snapshot.Data;
            if (create)
                _tableSnapshots.Insert(row);
            else
                _tableSnapshots.Update(row);
        }

        public void SaveSnapshot(EventStoreSnapshot snapshot)
        {
            bool createStream = _version == 0;
            bool createSnapshot = _snapshotVersion == 0;
            _snapshotVersion = snapshot.Version;
            SaveStreamToDb(createStream);
            SaveSnapshotToDb(snapshot, createSnapshot);
        }

        public int GetSnapshotVersion()
        {
            return _snapshotVersion;
        }

    }
}
