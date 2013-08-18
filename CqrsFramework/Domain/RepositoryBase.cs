using CqrsFramework.EventStore;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Domain
{
    public interface IRepository<TKey, TAgg>
        where TAgg : class, IAggregate, new()
    {
        TAgg Get(TKey key);
        void Save(TAgg aggregate, object context, RepositorySaveFlags repositorySaveFlags);
    }

    public abstract class RepositoryBase<TKey, TAgg> : IRepository<TKey, TAgg>
        where TAgg : class, IAggregate, new()
    {
        private IEventStore _store;
        private IMessagePublisher _bus;
        private IMessageSerializer _serializer;
        private IEventMessageFactory _eventMessageFactory;

        public RepositoryBase(IEventStore eventStore, IMessagePublisher busWriter, IEventMessageFactory eventMessageFactory, IMessageSerializer serializer)
        {
            this._store = eventStore;
            this._bus = busWriter;
            this._eventMessageFactory = eventMessageFactory;
            this._serializer = serializer;
        }

        public TAgg Get(TKey key)
        {
            var stream = _store.GetStream(EventStreamName(key), EventStreamOpenMode.Open);
            if (stream == null)
                return null;
            var storedSnapshot = stream.GetSnapshot();
            var snapshot = storedSnapshot == null ? null : _serializer.Deserialize(storedSnapshot.Data).Payload;
            var minVersion = storedSnapshot == null ? 1 : storedSnapshot.Version + 1;
            var events = stream.GetEvents(minVersion).Select(e => (IEvent)_serializer.Deserialize(e.Data).Payload);
            var agg = new TAgg();
            agg.LoadFromHistory(snapshot, events);
            return agg;
        }

        public void Save(TAgg aggregate, object context, RepositorySaveFlags repositorySaveFlags)
        {
            var streamName = EventStreamName(AggreagateId(aggregate));
            IEventStream stream = OpenSaveStream(streamName, repositorySaveFlags);
            var events = aggregate.GetEvents().ToArray();
            var eventMessages = new Message[events.Length];
            var storedEvents = new EventStoreEvent[events.Length];
            var aggregateVersionNow = AggregateVersion(aggregate);
            var aggregateVersionBeforeChanges = aggregateVersionNow - events.Length;
            for (int i = 0; i < events.Length; i++)
            {
                var eventVersion = aggregateVersionBeforeChanges + i + 1;
                eventMessages[i] = _eventMessageFactory.CreateMessage(events[i], context);
                var data = _serializer.Serialize(eventMessages[i]);
                storedEvents[i] = new EventStoreEvent() { Key = streamName, Version = eventVersion, Data = data };
            }
            var streamVersion = ExpectedVersion(repositorySaveFlags);
            stream.SaveEvents(streamVersion, storedEvents);
            for (int i = 0; i < events.Length; i++)
            {
                _eventMessageFactory.EnhanceMessage(eventMessages[i], storedEvents[i].Clock, storedEvents[i].Version);
                _bus.Publish(eventMessages[i]);
                _store.MarkAsPublished(storedEvents[i]);
            }
            var snapshotLimit = SnapshotLimit(repositorySaveFlags);
            bool makeSnapshot = ShouldMakeSnapshot(snapshotLimit, stream);
            if (makeSnapshot)
            {
                var snapshot = aggregate.GetSnapshot();
                if (snapshot != null)
                {
                    var snapshotData = _serializer.Serialize(new Message(snapshot));
                    var storedSnapshot = new EventStoreSnapshot() { Key = streamName, Version = aggregateVersionNow, Data = snapshotData };
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
        protected abstract int AggregateVersion(TAgg aggregate);
    }
}
