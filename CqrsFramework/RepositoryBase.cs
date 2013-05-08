﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework
{
    public abstract class RepositoryBase<TKey, TAgg>
        where TAgg : class, IAggregate, new()
    {
        private IEventStore _store;
        private IEventPublisher _bus;
        private IEventSerializer _serializer;

        public RepositoryBase(IEventStore eventStore, IEventPublisher busWriter, IEventSerializer eventSerializer)
        {
            this._store = eventStore;
            this._bus = busWriter;
            this._serializer = eventSerializer;
        }

        public TAgg Get(TKey key)
        {
            var stream = _store.GetStream(EventStreamName(key), EventStreamOpenMode.Open);
            if (stream == null)
                return null;
            var storedSnapshot = stream.GetSnapshot();
            var snapshot = storedSnapshot == null ? null : _serializer.DeserializeSnapshot(storedSnapshot);
            var minVersion = storedSnapshot == null ? 1 : storedSnapshot.Version + 1;
            var events = stream.GetEvents(minVersion).Select(e => _serializer.DeserializeEvent(e));
            var agg = new TAgg();
            agg.LoadFromHistory(snapshot, events);
            return agg;
        }

        public void Save(TAgg aggregate, RepositorySaveFlags repositorySaveFlags)
        {
            IEventStream stream = OpenSaveStream(EventStreamName(AggreagateId(aggregate)), repositorySaveFlags);
            var events = aggregate.GetEvents().ToArray();
            var storedEvents = events.Select(e => _serializer.SerializeEvent(e)).ToArray();
            var version = ExpectedVersion(repositorySaveFlags);
            stream.SaveEvents(version, storedEvents);
            for (int i = 0; i < events.Length; i++)
            {
                _bus.Publish(events[i]);
                _store.MarkAsPublished(storedEvents[i]);
            }
            var snapshotLimit = SnapshotLimit(repositorySaveFlags);
            bool makeSnapshot = ShouldMakeSnapshot(snapshotLimit, stream);
            if (makeSnapshot)
            {
                var snapshot = aggregate.GetSnapshot();
                if (snapshot != null)
                {
                    var storedSnapshot = _serializer.SerializeSnapshot(snapshot);
                    stream.SaveSnapshot(storedSnapshot);
                }
            }

            aggregate.Commit();
        }

        private bool ShouldMakeSnapshot(int snapshotLimit, IEventStream stream)
        {
            if (snapshotLimit == 0)
                return false;
            else if (snapshotLimit == 1)
                return true;
            else if (snapshotLimit < 0)
                return false;
            else
            {
                var snapshotVersion = stream.GetSnapshotVersion();
                var aggregateVersion = stream.GetCurrentVersion();
                return aggregateVersion >= snapshotLimit + snapshotVersion;
            }
        }

        private static int ExpectedVersion(RepositorySaveFlags flags)
        {
            if (flags.IsModeCreateNew())
                return 0;
            else if (flags.HasExpectedVersion())
                return flags.ExpectedVersion();
            else
                return -1;
        }

        private int SnapshotLimit(RepositorySaveFlags flags)
        {
            if (flags.HasSnapshotLimit())
                return flags.SnapshotLimit();
            else
                return 0;
        }

        private IEventStream OpenSaveStream(string name, RepositorySaveFlags repositorySaveFlags)
        {
            if (repositorySaveFlags.IsModeCreateNew())
                return _store.GetStream(name, EventStreamOpenMode.Create);
            else
                return _store.GetStream(name, EventStreamOpenMode.OpenExisting);
        }

        protected abstract string EventStreamName(TKey key);
        protected abstract TKey AggreagateId(TAgg aggregate);
    }
}
