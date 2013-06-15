using CqrsFramework.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    public interface IEventStore : IDisposable
    {
        IEnumerable<EventStoreEvent> GetUnpublishedEvents();
        IEventStream GetStream(string name, EventStreamOpenMode mode);
        void MarkAsPublished(EventStoreEvent @event);
        IEnumerable<EventStoreEvent> GetSince(long clock);
    }
    
    public interface IEventStream
    {
        int GetCurrentVersion();
        string GetName();
        EventStoreSnapshot GetSnapshot();
        IEnumerable<EventStoreEvent> GetEvents(int minVersion);
        void SaveEvents(int expectedVersion, EventStoreEvent[] events);
        void SaveSnapshot(EventStoreSnapshot snapshot);
        int GetSnapshotVersion();
    }

    public enum EventStreamOpenMode
    {
        Create,
        OpenExisting,
        Open
    }

    public class EventStoreEvent
    {
        public string Key;
        public int Version;
        public bool Published;
        public long Clock;
        public byte[] Data;

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 134532;
                hash ^= Key.GetHashCode();
                hash ^= Version * 851;
                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            var oth = obj as EventStoreEvent;
            if (oth == null)
                return false;
            return Key == oth.Key && Version == oth.Version && Published == oth.Published && EqualData(Data, oth.Data);
        }

        private bool EqualData(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
                return false;
            for (int i = 0; i < a.Length; i++ )
                if (a[i] != b[i])
                    return false;
            return true;
        }
    }

    public class EventStoreSnapshot
    {
        public string Key;
        public int Version;
        public byte[] Data;
    }

    public class EventStoreException : Exception
    {
        public EventStoreException(string message)
            : base(message)
        {
        }

        public static EventStoreException StreamAlreadyExists(string name)
        {
            return new EventStoreException(string.Format("Stream {0} already exists", name));
        }
        public static EventStoreException StreamDoesNotExist(string name)
        {
            return new EventStoreException(string.Format("Stream {0} does not exist", name));
        }
        public static EventStoreException UnexpectedVersion(string name, int expected, int actual)
        {
            return new EventStoreException(string.Format("Stream {0} has unexpected version {2}, expected {1}", name, expected, actual));
        }
    }
}
