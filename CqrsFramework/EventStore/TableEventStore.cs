using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.EventStore
{
    public class TableEventStore : IEventStore
    {
        private ITableProvider _tableStreams;
        private ITableProvider _tableEvents;
        private ITableProvider _tableSnapshots;

        public TableEventStore(ITableProvider tableStreams, ITableProvider tableEvents, ITableProvider tableSnapshots)
        {
            _tableStreams = tableStreams;
            _tableEvents = tableEvents; 
            _tableSnapshots = tableSnapshots;
        }

        public IEnumerable<EventStoreEvent> GetUnpublishedEvents()
        {
            var events = new List<EventStoreEvent>();
            var table = _tableEvents.GetRows().Where("published").Is(0).ToList();
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

        public IEventStream GetStream(string name, EventStreamOpenMode mode)
        {
            return TableEventStream.Create(name, mode, _tableStreams, _tableEvents, _tableSnapshots);
        }

        public void MarkAsPublished(EventStoreEvent @event)
        {
            var row = _tableEvents.GetRows().Where("name").Is(@event.Key).And("version").Is(@event.Version).Single();
            row["published"] = 1;
            _tableEvents.Update(row);
            @event.Published = true;
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock)
        {
            var events = new List<EventStoreEvent>();
            var table = _tableEvents.GetRows().Where("clock").IsAtLeast((int)clock).OrderBy(r => r.Get<int>("clock")).ToList();
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

        public void Dispose()
        {
            _tableSnapshots.Dispose();
            _tableEvents.Dispose();
            _tableStreams.Dispose();
        }
    }
}
