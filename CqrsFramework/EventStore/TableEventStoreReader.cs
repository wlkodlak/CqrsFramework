using CqrsFramework.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    public class TableEventStoreReader : IEventStoreReader
    {
        private ITableProvider _tableEvents;

        public TableEventStoreReader(ITableProvider tableEvents)
        {
            _tableEvents = tableEvents; 
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock, int maxCount)
        {
            var events = new List<EventStoreEvent>();
            var table = _tableEvents.GetRows().Where("clock").IsAtLeast((int)clock).OrderBy(r => r.Get<int>("clock")).Take(maxCount).ToList();
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
            _tableEvents.Dispose();
        }
    }
}
