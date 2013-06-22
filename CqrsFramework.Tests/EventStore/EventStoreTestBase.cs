using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Text;
using CqrsFramework.EventStore;

namespace CqrsFramework.Tests.EventStore
{
    public interface IEventStoreTestBuilder : IDisposable
    {
        IEventStore GetFull();
        IEventStoreReader GetReader();
        void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events);
        void Build();
    }

    public abstract class EventStoreTestBase
    {
        protected abstract IEventStoreTestBuilder CreateBuilder();

        [TestMethod]
        public void EmptyEventStoreHasNoUnpublishedEvents()
        {
            var builder = CreateBuilder();
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEnumerable<EventStoreEvent> events = store.GetUnpublishedEvents();
                CollectionAssert.AreEqual(new EventStoreEvent[0], events.ToList());
            }
        }

        [TestMethod]
        public void EventStoreWithUnpublishedEvents()
        {
            var builder = CreateBuilder();
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 124, 84, 21, 36 } };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 244, 94, 121, 6 } };
            builder.WithStream("agg-1", null, new[] { event1, event2 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEnumerable<EventStoreEvent> events = store.GetUnpublishedEvents();
                CollectionAssert.AreEqual(new[] { event1, event2 }, events.ToList());
                Assert.IsTrue(events.All(e => !e.Published));
            }
        }

        [TestMethod]
        public void CreateEmptyStream()
        {
            var builder = CreateBuilder();
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream stream = store.GetStream("agg-1", EventStreamOpenMode.Create);
                Assert.AreEqual(0, stream.GetCurrentVersion());
                Assert.AreEqual("agg-1", stream.GetName());
                Assert.AreEqual(null, stream.GetSnapshot());
                CollectionAssert.AreEqual(new EventStoreEvent[0], stream.GetEvents(0).ToList());
            }
        }

        [TestMethod]
        public void CreateStreamWithJustEvents()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 } };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 } };
            var event4 = new EventStoreEvent { Key = "agg-1", Version = 4, Data = new byte[] { 4, 22 } };
            var builder = CreateBuilder();
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.Create);
                streamToWrite.SaveEvents(0, new[] { event1, event2, event3, event4 });

                IEventStream streamToRead = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                var actualEvents = streamToRead.GetEvents(0).ToList();

                Assert.AreEqual(4, streamToRead.GetCurrentVersion());
                Assert.AreEqual("agg-1", streamToRead.GetName());
                AssertEqualStoredEvents(event1, actualEvents[0]);
                AssertEqualStoredEvents(event2, actualEvents[1]);
                AssertEqualStoredEvents(event3, actualEvents[2]);
                AssertEqualStoredEvents(event4, actualEvents[3]);
                Assert.AreEqual(4, actualEvents.Count);
            }
        }

        [TestMethod]
        public void CreateStreamWithSnapshot()
        {
            var snapshot = new EventStoreSnapshot { Key = "agg-1", Version = 2, Data = new byte[] { 2, 15, 17 } };
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 } };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 } };
            var event4 = new EventStoreEvent { Key = "agg-1", Version = 4, Data = new byte[] { 4, 22 } };
            var builder = CreateBuilder();
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.Create);
                streamToWrite.SaveEvents(0, new[] { event1, event2, event3, event4 });
                streamToWrite.SaveSnapshot(snapshot);

                IEventStream streamToRead = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                var actualSnapshot = streamToRead.GetSnapshot();

                Assert.AreEqual("agg-1", actualSnapshot.Key);
                Assert.AreEqual(2, actualSnapshot.Version);
                CollectionAssert.AreEqual(snapshot.Data, actualSnapshot.Data);
            }
        }

        [TestMethod]
        public void LoadJustEvents()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Published = true };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 }, Published = true };
            var event4 = new EventStoreEvent { Key = "agg-1", Version = 4, Data = new byte[] { 4, 22 }, Published = true };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1, event2, event3, event4 });
            builder.Build();
            using (var store = builder.GetFull())
            {

                IEventStream streamToRead = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                var actualEvents = streamToRead.GetEvents(0).ToList();

                Assert.AreEqual(4, streamToRead.GetCurrentVersion());
                Assert.AreEqual("agg-1", streamToRead.GetName());
                AssertEqualStoredEvents(event1, actualEvents[0]);
                AssertEqualStoredEvents(event2, actualEvents[1]);
                AssertEqualStoredEvents(event3, actualEvents[2]);
                AssertEqualStoredEvents(event4, actualEvents[3]);
                Assert.AreEqual(4, actualEvents.Count);
            }
        }

        [TestMethod]
        public void LoadSnapshotAndEventsAfter()
        {
            var snapshot = new EventStoreSnapshot { Key = "agg-1", Version = 2, Data = new byte[] { 2, 15, 17 } };
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Published = true };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 }, Published = true };
            var event4 = new EventStoreEvent { Key = "agg-1", Version = 4, Data = new byte[] { 4, 22 }, Published = true };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", snapshot, new[] { event1, event2, event3, event4 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToRead = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                var actualSnapshot = streamToRead.GetSnapshot();
                var actualEvents = streamToRead.GetEvents(3).ToList();

                Assert.AreEqual("agg-1", actualSnapshot.Key);
                Assert.AreEqual(2, actualSnapshot.Version);
                CollectionAssert.AreEqual(snapshot.Data, actualSnapshot.Data);
                Assert.AreEqual(4, streamToRead.GetCurrentVersion());
                Assert.AreEqual("agg-1", streamToRead.GetName());
                AssertEqualStoredEvents(event3, actualEvents[0]);
                AssertEqualStoredEvents(event4, actualEvents[1]);
                Assert.AreEqual(2, actualEvents.Count);
            }
        }

        [TestMethod]
        public void OpenExistingOnNonexistentFails()
        {
            try
            {
                var builder = CreateBuilder();
                builder.Build();
                using (var store = builder.GetFull())
                    store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                Assert.Fail("Expected EventStoreException");
            }
            catch (EventStoreException)
            {
            }
        }

        [TestMethod]
        public void OpenOnNonexistentFailsSilently()
        {
            var builder = CreateBuilder();
            builder.Build();
            using (var store = builder.GetFull())
                Assert.IsNull(store.GetStream("agg-1", EventStreamOpenMode.Open));
        }

        [TestMethod]
        public void CreateOnNonemptyExistingFails()
        {
            try
            {
                var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
                var builder = CreateBuilder();
                builder.WithStream("agg-1", null, new EventStoreEvent[] { event1 });
                builder.Build();
                using (var store = builder.GetFull())
                    store.GetStream("agg-1", EventStreamOpenMode.Create);
                Assert.Fail("Expected EventStoreException");
            }
            catch (EventStoreException)
            {
            }
        }

        [TestMethod]
        public void CreateOnEmptyExistingSucceeds()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new EventStoreEvent[0]);
            builder.Build();
            using (var store = builder.GetFull())
            {
                var stream = store.GetStream("agg-1", EventStreamOpenMode.Create);
                Assert.IsNotNull(stream);
            }
        }

        [TestMethod]
        public void UnexpectedVersion()
        {
            try
            {
                var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
                var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
                var builder = CreateBuilder();
                builder.WithStream("agg-1", null, new[] { event1 });
                builder.Build();
                using (var store = builder.GetFull())
                {
                    IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                    streamToWrite.SaveEvents(0, new[] { event2 });
                    Assert.Fail("Expected CqrsEventStoreException");
                }
            }
            catch (EventStoreException)
            {
            }
        }

        [TestMethod]
        public void MultipleStreams()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Published = true };
            var event3 = new EventStoreEvent { Key = "agg-2", Version = 1, Data = new byte[] { 3, 19 }, Published = true };
            var event4 = new EventStoreEvent { Key = "agg-3", Version = 1, Data = new byte[] { 4, 22 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1, event2 });
            builder.WithStream("agg-2", null, new[] { event3 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                store.GetStream("agg-3", EventStreamOpenMode.Create).SaveEvents(0, new[] { event4 });

                Assert.AreEqual(2, store.GetStream("agg-1", EventStreamOpenMode.OpenExisting).GetCurrentVersion());
                Assert.AreEqual(19, store.GetStream("agg-2", EventStreamOpenMode.OpenExisting).GetEvents(1).ToList()[0].Data[1]);
                Assert.AreEqual(22, store.GetStream("agg-3", EventStreamOpenMode.OpenExisting).GetEvents(1).ToList()[0].Data[1]);
            }
        }

        [TestMethod]
        public void OldEventsArePublished()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                CollectionAssert.AreEqual(new EventStoreEvent[0], store.GetUnpublishedEvents().ToList());
            }
        }

        [TestMethod]
        public void NewEventsAreUnpublished()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                streamToWrite.SaveEvents(1, new[] { event2 });
                CollectionAssert.AreEqual(new[] { event2 }, store.GetUnpublishedEvents().ToList());
            }
        }

        [TestMethod]
        public void SavingEventsDoesNotNeedToHaveExpectedVersion()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                streamToWrite.SaveEvents(-1, new[] { event2 });
            }
        }

        [TestMethod]
        public void SavingEventsAddsClock()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true, Clock = 40 };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                streamToWrite.SaveEvents(1, new[] { event2 });
                Assert.IsTrue(event2.Clock >= event1.Clock);
            }
        }

        [TestMethod]
        public void NewEventsAreMarkedAsPublished()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 } };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 } };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToWrite = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                streamToWrite.SaveEvents(1, new[] { event2, event3 });
                store.MarkAsPublished(event2);

                Assert.IsTrue(event2.Published);
                Assert.IsFalse(event3.Published);
                CollectionAssert.AreEqual(new[] { event3 }, store.GetUnpublishedEvents().ToList());
            }
        }


        protected void AssertEqualStoredEvents(EventStoreEvent expected, EventStoreEvent actual)
        {
            Assert.AreEqual(expected.Key, actual.Key, "Different keys - expected {0}, got {1}", expected.Key, actual.Key);
            Assert.AreEqual(expected.Version, actual.Version, "Different versions - expected {0}, got {1}", expected.Version, actual.Version);
            Assert.AreEqual(expected.Data.Length, actual.Data.Length, "Different data lengths - expected {0}, got {1}", expected.Data.Length, actual.Data.Length);
            CollectionAssert.AreEqual(expected.Data, actual.Data, "Different data");
        }

        [TestMethod]
        public void GetEventsSince()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true, Clock = 1 };
            var event2 = new EventStoreEvent { Key = "agg-2", Version = 1, Data = new byte[] { 3, 19 }, Clock = 2 };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Clock = 3 };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.WithStream("agg-2", null, new[] { event2 });
            builder.WithStream("agg-1", null, new[] { event3 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                var expected = new[] { event2, event3 };
                var actual = store.GetSince(event2.Clock).ToList();
                CollectionAssert.AreEqual(expected, actual);
            }
        }

        [TestMethod]
        public void GetEventsSinceUsingReader()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true, Clock = 1 };
            var event2 = new EventStoreEvent { Key = "agg-2", Version = 1, Data = new byte[] { 3, 19 }, Clock = 2 };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Clock = 3 };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.WithStream("agg-2", null, new[] { event2 });
            builder.WithStream("agg-1", null, new[] { event3 });
            builder.Build();
            using (var store = builder.GetReader())
            {
                var expected = new[] { event2, event3 };
                var actual = store.GetSince(event2.Clock).ToList();
                CollectionAssert.AreEqual(expected, actual);
            }
        }

        [TestMethod]
        public void VersionsAndExistenceCanPotentiallyBeOptimized()
        {
            var snapshot = new EventStoreSnapshot { Key = "agg-1", Version = 2, Data = new byte[] { 2, 15, 17 } };
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true };
            var event2 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Published = true };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 3, Data = new byte[] { 3, 19 }, Published = true };
            var event4 = new EventStoreEvent { Key = "agg-1", Version = 4, Data = new byte[] { 4, 22 }, Published = true };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", snapshot, new[] { event1, event2, event3, event4 });
            builder.Build();
            using (var store = builder.GetFull())
            {
                IEventStream streamToRead = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                var snapshotVersion = streamToRead.GetSnapshotVersion();
                var aggregateVersion = streamToRead.GetCurrentVersion();

                var actualSnapshot = streamToRead.GetSnapshot();
                var actualEvents = streamToRead.GetEvents(3).ToList();

                Assert.AreEqual("agg-1", actualSnapshot.Key);
                Assert.AreEqual(2, actualSnapshot.Version);
                CollectionAssert.AreEqual(snapshot.Data, actualSnapshot.Data);
                Assert.AreEqual(2, snapshotVersion);
                Assert.AreEqual(4, aggregateVersion);
                Assert.AreEqual("agg-1", streamToRead.GetName());
                AssertEqualStoredEvents(event3, actualEvents[0]);
                AssertEqualStoredEvents(event4, actualEvents[1]);
                Assert.AreEqual(2, actualEvents.Count);
            }
        }

        [TestMethod]
        public void ReaderHandlesNewEvents()
        {
            var event1 = new EventStoreEvent { Key = "agg-1", Version = 1, Data = new byte[] { 1, 15 }, Published = true, Clock = 1 };
            var event2 = new EventStoreEvent { Key = "agg-2", Version = 1, Data = new byte[] { 3, 19 }, Clock = 2 };
            var event3 = new EventStoreEvent { Key = "agg-1", Version = 2, Data = new byte[] { 2, 17 }, Clock = 3 };
            var builder = CreateBuilder();
            builder.WithStream("agg-1", null, new[] { event1 });
            builder.WithStream("agg-2", null, new[] { event2 });
            builder.Build();
            using (var reader = builder.GetReader())
            using (var store = builder.GetFull())
            {
                var stream = store.GetStream("agg-1", EventStreamOpenMode.OpenExisting);
                stream.SaveEvents(1, new EventStoreEvent[] { event3 });
                var expected = new[] { event2, event3 };
                var actual = reader.GetSince(event2.Clock).ToList();
                CollectionAssert.AreEqual(expected, actual);
            }
        }
    }

}
