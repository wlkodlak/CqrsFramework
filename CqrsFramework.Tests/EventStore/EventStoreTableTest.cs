using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data;
using System.Linq;
using Moq;
using System.Collections.Generic;
using CqrsFramework.Infrastructure;
using CqrsFramework.EventStore;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreTableTest : EventStoreTestBase
    {
        protected override IEventStoreTestBuilder CreateBuilder()
        {
            return new Builder();
        }

        private class Builder : IEventStoreTestBuilder
        {
            private DataTable _dataTableStreams;
            private DataTable _dataTableEvents;
            private DataTable _dataTableSnapshots;

            private MemoryTableProvider _tableStreams;
            private MemoryTableProvider _tableEvents;
            private MemoryTableProvider _tableSnapshots;

            public Builder()
            {
                _dataTableStreams = new DataTable("es_streams");
                _dataTableStreams.Columns.Add("id", typeof(int));
                _dataTableStreams.Columns.Add("name", typeof(string));
                _dataTableStreams.Columns.Add("version", typeof(int));
                _dataTableStreams.Columns.Add("snapshotversion", typeof(int));
                _dataTableSnapshots = new DataTable("es_snapshots");
                _dataTableSnapshots.Columns.Add("id", typeof(int));
                _dataTableSnapshots.Columns.Add("name", typeof(string));
                _dataTableSnapshots.Columns.Add("snapshot", typeof(byte[]));
                _dataTableEvents = new DataTable("es_events");
                _dataTableEvents.Columns.Add("id", typeof(int));
                _dataTableEvents.Columns.Add("name", typeof(string));
                _dataTableEvents.Columns.Add("version", typeof(int));
                _dataTableEvents.Columns.Add("clock", typeof(int));
                _dataTableEvents.Columns.Add("data", typeof(byte[]));
                _dataTableEvents.Columns.Add("published", typeof(int));

                _tableStreams = new MemoryTableProvider(_dataTableStreams, null);
                _tableEvents = new MemoryTableProvider(_dataTableEvents, null);
                _tableSnapshots = new MemoryTableProvider(_dataTableSnapshots, null);
            }

            public IEventStore Build()
            {
                return new TableEventStore(_tableStreams, _tableEvents, _tableSnapshots);
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                int version = 0;
                if (snapshot != null)
                    version = snapshot.Version;
                if (events.Length > 0)
                    version = events.Max(e => e.Version);

                var rowStream = _tableStreams.NewRow();
                rowStream["name"] = name;
                rowStream["version"] = version;
                rowStream["snapshotversion"] = snapshot == null ? 0 : snapshot.Version;
                _tableStreams.Insert(rowStream);

                var rowSnapshot = _tableSnapshots.NewRow();
                rowSnapshot["name"] = name;
                rowSnapshot["snapshot"] = snapshot == null ? null : snapshot.Data;
                _tableSnapshots.Insert(rowSnapshot);

                foreach (var @event in events)
                {
                    var rowEvent = _tableEvents.NewRow();
                    rowEvent["name"] = name;
                    rowEvent["version"] = @event.Version;
                    rowEvent["data"] = @event.Data;
                    rowEvent["published"] = @event.Published ? 1 : 0;
                    rowEvent["clock"] = (int)@event.Clock;
                    _tableEvents.Insert(rowEvent);
                    @event.Clock = rowEvent.RowNumber;
                }

                _dataTableStreams.AcceptChanges();
                _dataTableSnapshots.AcceptChanges();
                _dataTableEvents.AcceptChanges();
            }

            public void Dispose()
            {
                _dataTableEvents.Dispose();
                _dataTableSnapshots.Dispose();
                _dataTableStreams.Dispose();
                _tableEvents.Dispose();
                _tableSnapshots.Dispose();
                _tableStreams.Dispose();
            }
        }
    }
}
