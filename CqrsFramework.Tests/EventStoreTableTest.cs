using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data;
using System.Linq;
using Moq;
using System.Collections.Generic;
using CqrsFramework.InMemory;
using CqrsFramework.InTable;

namespace CqrsFramework.Tests
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
                _dataTableStreams.Columns.Add("name", typeof(string));
                _dataTableStreams.Columns.Add("version", typeof(int));
                _dataTableStreams.Columns.Add("snapshotversion", typeof(int));
                _dataTableSnapshots = new DataTable("es_snapshots");
                _dataTableSnapshots.Columns.Add("name", typeof(string));
                _dataTableSnapshots.Columns.Add("snapshot", typeof(byte[]));
                _dataTableEvents = new DataTable("es_events");
                _dataTableEvents.Columns.Add("name", typeof(string));
                _dataTableEvents.Columns.Add("version", typeof(int));
                _dataTableEvents.Columns.Add("clock", typeof(int));
                _dataTableEvents.Columns.Add("data", typeof(byte[]));
                _dataTableEvents.Columns.Add("published", typeof(int));

                _tableStreams = new MemoryTableProvider(_dataTableStreams);
                _tableEvents = new MemoryTableProvider(_dataTableEvents);
                _tableSnapshots = new MemoryTableProvider(_dataTableSnapshots);
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

                var rowStream = _dataTableStreams.NewRow();
                rowStream.SetField<string>("name", name);
                rowStream.SetField<int>("version", version);
                rowStream.SetField<int>("snapshotversion", snapshot == null ? 0 : snapshot.Version);
                _dataTableStreams.Rows.Add(rowStream);

                var rowSnapshot = _dataTableSnapshots.NewRow();
                rowSnapshot.SetField<string>("name", name);
                rowSnapshot.SetField<byte[]>("snapshot", snapshot == null ? null : snapshot.Data);
                _dataTableSnapshots.Rows.Add(rowSnapshot);

                foreach (var @event in events)
                {
                    var rowEvent = _dataTableEvents.NewRow();
                    rowEvent.SetField<string>("name", name);
                    rowEvent.SetField<int>("version", @event.Version);
                    rowEvent.SetField<byte[]>("data", @event.Data);
                    rowEvent.SetField<int>("published", @event.Published ? 1 : 0);
                    rowEvent.SetField<int>("clock", (int)@event.Clock);
                    _dataTableEvents.Rows.Add(rowEvent);
                }

                _dataTableStreams.AcceptChanges();
                _dataTableSnapshots.AcceptChanges();
                _dataTableEvents.AcceptChanges();
            }
        }
    }
}
