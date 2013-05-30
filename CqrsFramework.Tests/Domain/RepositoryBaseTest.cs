using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;
using System.Xml.Linq;
using System.IO;
using System.Linq;

namespace CqrsFramework.Tests.Domain
{
    [TestClass]
    public class RepositoryBaseTest
    {
        private class TestRepository : RepositoryBase<int, TestAggregate>
        {
            public TestRepository(IEventStore eventStore, IMessagePublisher busWriter, IEventMessageFactory messageFactory, IEventStoreSerializer eventSerializer)
                : base(eventStore, busWriter, messageFactory, eventSerializer)
            {
            }

            protected override string EventStreamName(int key)
            {
                return string.Format("TestAggregate:{0}", key);
            }

            protected override int AggreagateId(TestAggregate aggregate)
            {
                return aggregate.Id;
            }
        }

        private class TestAggregate : IAggregate
        {
            public int Id;
            public int Version;
            public TestSnapshot Snapshot;
            public List<IEvent> HistoricEvents = new List<IEvent>();
            public List<IEvent> NewEvents = new List<IEvent>();

            public void LoadFromHistory(object snapshot, IEnumerable<IEvent> history)
            {
                Snapshot = (TestSnapshot)snapshot;
                if (Snapshot != null)
                    HistoricEvents.AddRange(Snapshot.Historic);
                foreach (TestEvent @event in history)
                {
                    HistoricEvents.Add(@event);
                    Id = @event.Aggregate;
                    Version = @event.Version;
                }
            }

            public IEnumerable<IEvent> GetEvents()
            {
                return NewEvents;
            }

            public void Commit()
            {
                HistoricEvents.AddRange(NewEvents);
                NewEvents.Clear();
            }

            public object GetSnapshot()
            {
                return Snapshot;
            }

            public void PublishEvents(IEnumerable<IEvent> events)
            {
                foreach (TestEvent @event in events)
                {
                    NewEvents.Add(@event);
                    Id = @event.Aggregate;
                    Version = @event.Version;
                }
            }
        }

        private class TestEvent : IEvent
        {
            public int Aggregate { get; set; }
            public int Version { get; set; }

            public TestEvent(int aggregate, int version)
            {
                this.Aggregate = aggregate;
                this.Version = version;
            }

            public EventStoreEvent GetStoredEvent()
            {
                var data = new byte[2];
                data[0] = 100;
                data[1] = (byte)Version;
                return new EventStoreEvent
                {
                    Key = string.Format("TestAggregate:{0}", Aggregate),
                    Published = true,
                    Clock = Version,
                    Version = Version,
                    Data = data
                };
            }
        }

        private class TestSnapshot
        {
            public int Aggregate;
            public int Version;
            public List<IEvent> Historic = new List<IEvent>();

            public EventStoreSnapshot GetStoredSnapshot()
            {
                XElement elem = new XElement("Snapshot",
                    new XAttribute("Id", Aggregate),
                    new XAttribute("Version", Version),
                    Historic.Cast<TestEvent>().Select(e => 
                        new XElement("Event", new XAttribute("Version", e.Version))),
                    null);

                var stream = new MemoryStream();
                elem.Save(stream);

                return new EventStoreSnapshot
                {
                    Key = string.Format("TestAggregate:{0}", Aggregate),
                    Version = Version,
                    Data = stream.ToArray()
                };
            }
        }

        private static Message EventMessageFactoryMethod(IEvent @event, object context)
        {
            var message = new Message(@event);
            return message;
        }

        [TestMethod]
        public void LoadingNonExistentReturnsNull()
        {
            var storeMock = new Mock<IEventStore>();
            var busMock = new Mock<IMessagePublisher>();
            var serializerMock = new Mock<IEventStoreSerializer>();
            var factoryMock = new Mock<IEventMessageFactory>();

            storeMock
                .Setup(s => s.GetStream("TestAggregate:847", EventStreamOpenMode.Open))
                .Returns((IEventStream)null)
                .Verifiable();
            var repository = new TestRepository(storeMock.Object, busMock.Object, factoryMock.Object, serializerMock.Object);
            Assert.IsNull(repository.Get(847));
            storeMock.Verify();
        }

        [TestMethod]
        public void LoadingExistingWithoutSnapshot()
        {
            var storeMock = new Mock<IEventStore>();
            var busMock = new Mock<IMessagePublisher>();
            var serializerMock = new Mock<IEventStoreSerializer>();
            var streamMock = new Mock<IEventStream>();
            var factoryMock = new Mock<IEventMessageFactory>();

            var event1 = new TestEvent(157, 1);
            var event2 = new TestEvent(157, 2);
            var stored1 = event1.GetStoredEvent();
            var stored2 = event2.GetStoredEvent();

            storeMock
                .Setup(s => s.GetStream("TestAggregate:157", EventStreamOpenMode.Open))
                .Returns(streamMock.Object)
                .Verifiable();
            streamMock
                .Setup(s => s.GetSnapshot())
                .Returns((EventStoreSnapshot)null)
                .Verifiable();
            streamMock
                .Setup(s => s.GetEvents(1))
                .Returns(new[] { stored1, stored2 })
                .Verifiable();
            serializerMock
                .Setup(s => s.DeserializeEvent(stored1))
                .Returns(EventMessageFactoryMethod(event1, null))
                .Verifiable();
            serializerMock
                .Setup(s => s.DeserializeEvent(stored2))
                .Returns(EventMessageFactoryMethod(event2, null))
                .Verifiable();

            var repository = new TestRepository(storeMock.Object, busMock.Object, factoryMock.Object, serializerMock.Object);
            var aggregate = repository.Get(157);

            storeMock.Verify();
            streamMock.Verify();
            serializerMock.Verify();

            Assert.IsNull(aggregate.Snapshot);
            CollectionAssert.AreEqual(new[] { event1, event2 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
        }

        [TestMethod]
        public void LoadingExistingWithSnapshot()
        {
            var storeMock = new Mock<IEventStore>();
            var busMock = new Mock<IMessagePublisher>();
            var serializerMock = new Mock<IEventStoreSerializer>();
            var streamMock = new Mock<IEventStream>();
            var factoryMock = new Mock<IEventMessageFactory>();

            var snapshot = new TestSnapshot();
            var event1 = new TestEvent(111, 1);
            var event2 = new TestEvent(111, 2);
            var event3 = new TestEvent(111, 3);
            snapshot.Aggregate = 111;
            snapshot.Version = 2;
            snapshot.Historic.Add(event1);
            snapshot.Historic.Add(event2);
            var storedSnapshot = snapshot.GetStoredSnapshot();
            var stored3 = event3.GetStoredEvent();

            storeMock
                .Setup(s => s.GetStream("TestAggregate:111", EventStreamOpenMode.Open))
                .Returns(streamMock.Object)
                .Verifiable();
            streamMock
                .Setup(s => s.GetSnapshot())
                .Returns(storedSnapshot)
                .Verifiable();
            serializerMock
                .Setup(s => s.DeserializeSnapshot(storedSnapshot))
                .Returns(snapshot)
                .Verifiable();
            streamMock
                .Setup(s => s.GetEvents(3))
                .Returns(new[] { stored3 })
                .Verifiable();
            serializerMock
                .Setup(s => s.DeserializeEvent(stored3))
                .Returns(EventMessageFactoryMethod(event3, null))
                .Verifiable();

            var repository = new TestRepository(storeMock.Object, busMock.Object, factoryMock.Object, serializerMock.Object);
            var aggregate = repository.Get(111);

            storeMock.Verify();
            streamMock.Verify();
            serializerMock.Verify();

            Assert.IsNotNull(aggregate.Snapshot);
            Assert.AreEqual(2, aggregate.Snapshot.Version);
            CollectionAssert.AreEqual(new[] { event1, event2 }, aggregate.Snapshot.Historic);
            CollectionAssert.AreEqual(new[] { event1, event2, event3 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
        }

        [TestMethod]
        public void SavingNewAggregate()
        {
            var aggregate = new TestAggregate();
            var event1 = new TestEvent(111, 1);
            var event2 = new TestEvent(111, 2);
            var context = "Hello";
            var message1 = EventMessageFactoryMethod(event1, context);
            var message2 = EventMessageFactoryMethod(event2, context);
            var stored1 = event1.GetStoredEvent();
            var stored2 = event2.GetStoredEvent();
            aggregate.PublishEvents(new[] { event1, event2 });

            var store = new Mock<IEventStore>();
            var stream = new Mock<IEventStream>();
            var bus = new Mock<IMessagePublisher>();
            var serializer = new Mock<IEventStoreSerializer>();
            var factory = new Mock<IEventMessageFactory>();
            var busSequence = new MockSequence();

            store
                .Setup(s => s.GetStream("TestAggregate:111", EventStreamOpenMode.Create))
                .Returns(stream.Object)
                .Verifiable();
            bus.InSequence(busSequence).Setup(b => b.Publish(message1)).Verifiable();
            bus.InSequence(busSequence).Setup(b => b.Publish(message2)).Verifiable();
            serializer
                .Setup(s => s.SerializeEvent(message1))
                .Returns(stored1)
                .Verifiable();
            serializer
                .Setup(s => s.SerializeEvent(message2))
                .Returns(stored2)
                .Verifiable();
            factory
                .Setup(f => f.CreateMessage(event1, context))
                .Returns(message1)
                .Verifiable();
            factory
                .Setup(f => f.CreateMessage(event2, context))
                .Returns(message2)
                .Verifiable();

            stream
                .Setup(s => s.SaveEvents(0, 
                    It.Is<EventStoreEvent[]>(l => 
                        l.Length == 2 &&
                        l[0].Equals(stored1) &&
                        l[1].Equals(stored2))))
                .Verifiable();
            store
                .Setup(s => s.MarkAsPublished(stored1))
                .Verifiable();
            store
                .Setup(s => s.MarkAsPublished(stored2))
                .Verifiable();

            var repository = new TestRepository(store.Object, bus.Object, factory.Object, serializer.Object);
            repository.Save(aggregate, context, RepositorySaveFlags.Create.WithoutSnapshot);

            store.Verify();
            stream.Verify();
            bus.Verify();
            serializer.Verify();
            factory.Verify();

            CollectionAssert.AreEqual(new[] { event1, event2 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
            Assert.AreEqual(111, aggregate.Id);
        }

        [TestMethod]
        public void SavingExistingAggregate()
        {
            var context = "Saving context";
            var event1 = new TestEvent(584, 1);
            var event2 = new TestEvent(584, 2);
            var event3 = new TestEvent(584, 3);
            var message1 = EventMessageFactoryMethod(event1, context);
            var message2 = EventMessageFactoryMethod(event2, context);
            var message3 = EventMessageFactoryMethod(event3, context);
            var stored1 = event1.GetStoredEvent();
            var stored2 = event2.GetStoredEvent();
            var stored3 = event3.GetStoredEvent();

            var store = new Mock<IEventStore>();
            var stream = new Mock<IEventStream>();
            var bus = new Mock<IMessagePublisher>();
            var serializer = new Mock<IEventStoreSerializer>();
            var factory = new Mock<IEventMessageFactory>();

            store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.Open))
                .Returns(stream.Object);
            serializer.Setup(s => s.DeserializeEvent(stored1)).Returns(message1);
            serializer.Setup(s => s.DeserializeEvent(stored2)).Returns(message2);
            stream.Setup(s => s.GetSnapshot()).Returns((EventStoreSnapshot)null);
            stream.Setup(s => s.GetEvents(1)).Returns(new[] { stored1, stored2 });

            store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.OpenExisting))
                .Returns(stream.Object);
            serializer.Setup(s => s.SerializeEvent(message3)).Returns(stored3).Verifiable();
            stream.Setup(s => s.SaveEvents(2, It.Is<EventStoreEvent[]>(ea => ea.Single().Equals(stored3)))).Verifiable();
            bus.Setup(b => b.Publish(message3)).Verifiable();
            store.Setup(s => s.MarkAsPublished(stored3)).Verifiable();
            factory.Setup(f => f.CreateMessage(event3, context)).Returns(message3).Verifiable();

            var repository = new TestRepository(store.Object, bus.Object, factory.Object, serializer.Object);
            var aggregate = repository.Get(584);
            aggregate.PublishEvents(new[] { event3 });
            repository.Save(aggregate, context, RepositorySaveFlags.Append.ToVersion(2).WithoutSnapshot);

            store.Verify();
            stream.Verify();
            bus.Verify();
            serializer.Verify();
            factory.Verify();

            CollectionAssert.AreEqual(new[] { event1, event2, event3 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
            Assert.AreEqual(3, aggregate.Version);
        }

        [TestMethod]
        public void SavingExistingWithSnapshot()
        {
            var context = "Context with snapshot";
            var event1 = new TestEvent(584, 1);
            var event2 = new TestEvent(584, 2);
            var event3 = new TestEvent(584, 3);
            var message1 = EventMessageFactoryMethod(event1, context);
            var message2 = EventMessageFactoryMethod(event2, context);
            var message3 = EventMessageFactoryMethod(event3, context);
            var stored1 = event1.GetStoredEvent();
            var stored2 = event2.GetStoredEvent();
            var stored3 = event3.GetStoredEvent();
            var snapshot = new TestSnapshot() { Aggregate = 584, Version = 3 };
            snapshot.Historic.AddRange(new[] { event1, event2, event3 });
            var storedSnapshot = snapshot.GetStoredSnapshot();

            var store = new Mock<IEventStore>();
            var stream = new Mock<IEventStream>();
            var bus = new Mock<IMessagePublisher>();
            var serializer = new Mock<IEventStoreSerializer>();
            var factory = new Mock<IEventMessageFactory>();

            store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.Open))
                .Returns(stream.Object);
            serializer.Setup(s => s.DeserializeEvent(stored1)).Returns(message1);
            serializer.Setup(s => s.DeserializeEvent(stored2)).Returns(message2);
            stream.Setup(s => s.GetSnapshot()).Returns((EventStoreSnapshot)null);
            stream.Setup(s => s.GetEvents(1)).Returns(new[] { stored1, stored2 });

            store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.OpenExisting))
                .Returns(stream.Object);
            serializer.Setup(s => s.SerializeEvent(message3)).Returns(stored3).Verifiable();
            serializer.Setup(s => s.SerializeSnapshot(snapshot)).Returns(storedSnapshot).Verifiable();
            stream
                .Setup(s => s.SaveEvents(
                    It.Is<int>(i => i == -1 || i == 2), 
                    It.Is<EventStoreEvent[]>(ea => ea.Single().Equals(stored3))
                    )).Verifiable();
            bus.Setup(b => b.Publish(message3)).Verifiable();
            store.Setup(s => s.MarkAsPublished(stored3)).Verifiable();
            stream.Setup(s => s.SaveSnapshot(storedSnapshot)).Verifiable();
            factory.Setup(f => f.CreateMessage(event3, context)).Returns(message3).Verifiable();

            var repository = new TestRepository(store.Object, bus.Object, factory.Object, serializer.Object);
            var aggregate = repository.Get(584);
            aggregate.PublishEvents(new[] { event3 });
            aggregate.Snapshot = snapshot;
            repository.Save(aggregate, context, RepositorySaveFlags.Append.WithSnapshot);

            store.Verify();
            stream.Verify();
            bus.Verify();
            serializer.Verify();

            CollectionAssert.AreEqual(new[] { event1, event2, event3 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
            Assert.AreEqual(3, aggregate.Version);
        }

    }
}
