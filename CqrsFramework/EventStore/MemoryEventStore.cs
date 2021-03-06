﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    public class MemoryEventStore : IEventStore
    {
        private object _lock = new object();
        private List<EventStoreEvent> _unpublished = new List<EventStoreEvent>();
        private Dictionary<string, MemoryEventStream> _streams = new Dictionary<string, MemoryEventStream>();
        private long _clock = 0;

        internal void AddToUnpublished(IEnumerable<EventStoreEvent> events)
        {
            _unpublished.AddRange(events);
        }

        public void SetupStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
        {
            MemoryEventStream stream;
            if (!_streams.TryGetValue(name, out stream))
                _streams[name] = stream = new MemoryEventStream(this, name, snapshot, events);
            else
                stream.InternalSetup(snapshot, events);
            _unpublished.AddRange(events.Where(e => !e.Published));
            if (events.Length > 0)
                UpdateClock(events.Max(e => e.Clock));
        }

        public IEnumerable<EventStoreEvent> GetUnpublishedEvents()
        {
            lock (_lock)
                return _unpublished.ToList();
        }

        public IEventStream GetStream(string name, EventStreamOpenMode mode)
        {
            lock (_lock)
            {
                MemoryEventStream stream;
                if (_streams.TryGetValue(name, out stream))
                {
                    if (mode == EventStreamOpenMode.OpenExisting)
                        return stream;
                    else
                    {
                        if (stream.GetCurrentVersion() == 0)
                            return stream;
                        else
                            throw EventStoreException.StreamAlreadyExists(name);
                    }
                }
                else
                {
                    if (mode == EventStreamOpenMode.OpenExisting)
                        throw EventStoreException.StreamDoesNotExist(name);
                    else if (mode == EventStreamOpenMode.Open)
                        return null;
                    else
                    {
                        stream = new MemoryEventStream(this, name, null, new EventStoreEvent[0]);
                        _streams[name] = stream;
                        return stream;
                    }
                }
            }
        }

        public void MarkAsPublished(EventStoreEvent @event)
        {
            lock (_lock)
            {
                @event.Published = true;
                _unpublished.Remove(@event);
            }
        }

        public void Dispose()
        {
        }


        public IEnumerable<EventStoreEvent> GetSince(long clock, int maxCount)
        {
            return _streams
                .SelectMany(e => e.Value.GetSince(clock))
                .OrderBy(e => e.Clock)
                .Take(maxCount)
                .ToList();
        }

        public void UpdateClock(long clock)
        {
            if (_clock < clock)
                _clock = clock;
        }

        public long GetClock()
        {
            return _clock;
        }
    }
}
