using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;
using System.Xml.Linq;
using System.IO;
using System.Linq;
using CqrsFramework.Domain;
using CqrsFramework.EventStore;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.Domain
{
    [TestClass]
    public class RepositoryBaseTest
    {
        private class TestRepository : RepositoryBase<int, TestAggregate>
        {
            public TestRepository(IEventStore eventStore, IMessagePublisher busWriter, IEventMessageFactory messageFactory, IMessageSerializer eventSerializer)
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

            protected override int AggregateVersion(TestAggregate aggregate)
            {
                return aggregate.Version;
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

            public byte[] GetBytes()
            {
                var stream = new MemoryStream();
                var elem = new XElement("TestEvent",
                    new XAttribute("Agg", Aggregate),
                    new XAttribute("Ver", Version));
                elem.Save(stream);
                return stream.ToArray();
            }

            public EventStoreEvent GetStored()
            {
                return new EventStoreEvent
                {
                    Data = GetBytes(),
                    Version = Version,
                    Published = false,
                    Key = string.Format("TestAggregate:{0}", Aggregate),
                    Clock = 100
                };
            }

            public Message GetMessage()
            {
                return new Message(this);
            }
        }

        private class TestSnapshot
        {
            public int Aggregate;
            public int Version;
            public List<IEvent> Historic = new List<IEvent>();

            public byte[] GetBytes()
            {
                XElement elem = new XElement("Snapshot",
                    new XAttribute("Id", Aggregate),
                    new XAttribute("Version", Version),
                    Historic.Cast<TestEvent>().Select(e =>
                        new XElement("Event", new XAttribute("Version", e.Version))),
                    null);

                var stream = new MemoryStream();
                elem.Save(stream);
                return stream.ToArray();
            }

            public EventStoreSnapshot GetStored()
            {
                return new EventStoreSnapshot
                {
                    Data = GetBytes(),
                    Version = Version,
                    Key = string.Format("TestAggregate:{0}", Aggregate)
                };
            }

            public Message GetMessage()
            {
                return new Message(this);
            }
        }

        private static Message EventMessageFactoryMethod(IEvent @event, object context)
        {
            var message = new Message(@event);
            return message;
        }

        private MockRepository _repo;
        private Mock<IEventStore> _store;
        private Mock<IEventStream> _stream;
        private Mock<IMessagePublisher> _bus;
        private Mock<IMessageSerializer> _serializer;
        private Mock<IEventMessageFactory> _factory;

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            _store = _repo.Create<IEventStore>();
            _stream = _repo.Create<IEventStream>();
            _bus = _repo.Create<IMessagePublisher>();
            _serializer = _repo.Create<IMessageSerializer>();
            _factory = _repo.Create<IEventMessageFactory>();
        }

        [TestMethod]
        public void LoadingNonExistentReturnsNull()
        {
            _store
                .Setup(s => s.GetStream("TestAggregate:847", EventStreamOpenMode.Open))
                .Returns((IEventStream)null)
                .Verifiable();
            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            Assert.IsNull(repository.Get(847));
            _repo.Verify();
        }

        [TestMethod]
        public void LoadingExistingWithoutSnapshot()
        {
            var event1 = new TestEvent(157, 1);
            var event2 = new TestEvent(157, 2);
            var message1 = event1.GetMessage();
            var message2 = event2.GetMessage();
            var stored1 = event1.GetStored();
            var stored2 = event2.GetStored();

            _store.Setup(s => s.GetStream("TestAggregate:157", EventStreamOpenMode.Open)).Returns(_stream.Object).Verifiable();
            _stream.Setup(s => s.GetSnapshot()).Returns((EventStoreSnapshot)null).Verifiable();
            _stream.Setup(s => s.GetEvents(1)).Returns(new[] { stored1, stored2 }).Verifiable();
            _serializer.Setup(s => s.Deserialize(stored1.Data)).Returns(message1).Verifiable();
            _serializer.Setup(s => s.Deserialize(stored2.Data)).Returns(message2).Verifiable();

            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            var aggregate = repository.Get(157);

            _repo.Verify();
            Assert.IsNull(aggregate.Snapshot);
            CollectionAssert.AreEqual(new[] { event1, event2 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
        }

        [TestMethod]
        public void LoadingExistingWithSnapshot()
        {
            var snapshot = new TestSnapshot();
            var event1 = new TestEvent(111, 1);
            var event2 = new TestEvent(111, 2);
            var event3 = new TestEvent(111, 3);
            snapshot.Aggregate = 111;
            snapshot.Version = 2;
            snapshot.Historic.Add(event1);
            snapshot.Historic.Add(event2);
            var storedSnapshot = snapshot.GetStored();
            var stored3 = event3.GetStored();

            _store.Setup(s => s.GetStream("TestAggregate:111", EventStreamOpenMode.Open)).Returns(_stream.Object).Verifiable();
            _stream.Setup(s => s.GetSnapshot()).Returns(storedSnapshot).Verifiable();
            _serializer.Setup(s => s.Deserialize(storedSnapshot.Data)).Returns(snapshot.GetMessage()).Verifiable();
            _stream.Setup(s => s.GetEvents(3)).Returns(new[] { stored3 }).Verifiable();
            _serializer.Setup(s => s.Deserialize(stored3.Data)).Returns(event3.GetMessage()).Verifiable();

            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            var aggregate = repository.Get(111);

            _repo.Verify();
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
            var message1 = event1.GetMessage();
            var message2 = event2.GetMessage();
            var stored1 = event1.GetStored();
            var stored2 = event2.GetStored();
            var context = "Hello";
            aggregate.PublishEvents(new[] { event1, event2 });

            _store.Setup(s => s.GetClock()).Returns(100).Verifiable();
            _store.Setup(s => s.GetStream("TestAggregate:111", EventStreamOpenMode.Create)).Returns(_stream.Object).Verifiable();
            _factory.Setup(f => f.CreateMessage(event1, context, 100, event1.Version)).Returns(message1).Verifiable();
            _factory.Setup(f => f.CreateMessage(event2, context, 100, event2.Version)).Returns(message2).Verifiable();
            var busSequence = 0;
            _bus
                .Setup(b => b.Publish(It.Is<Message>(m => m.Payload == event1)))
                .Callback<Message>(m => { Assert.AreEqual(0, busSequence, "Bus sequence"); busSequence++; })
                .Verifiable();
            _bus
                .Setup(b => b.Publish(It.Is<Message>(m => m.Payload == event2)))
                .Callback<Message>(m => { Assert.AreEqual(1, busSequence, "Bus sequence"); busSequence++; })
                .Verifiable();
            _serializer.Setup(s => s.Serialize(message1)).Returns(event1.GetBytes()).Verifiable();
            _serializer.Setup(s => s.Serialize(message2)).Returns(event2.GetBytes()).Verifiable();

            _stream
                .Setup(s => s.SaveEvents(0, It.IsAny<EventStoreEvent[]>()))
                .Callback<int, EventStoreEvent[]>((v, e) => 
                    {
                        Assert.AreEqual(2, e.Length, "Length");
                        AssertExtension.AreEqual(stored1, e[0], "Event1");
                        AssertExtension.AreEqual(stored2, e[1], "Event2");
                    })
                .Verifiable();
            _store.Setup(s => s.MarkAsPublished(stored1)).Verifiable();
            _store.Setup(s => s.MarkAsPublished(stored2)).Verifiable();

            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            repository.Save(aggregate, context, RepositorySaveFlags.Create.WithoutSnapshot);

            _repo.Verify();
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
            var message1 = event1.GetMessage();
            var message2 = event2.GetMessage();
            var message3 = event3.GetMessage();
            var stored1 = event1.GetStored();
            var stored2 = event2.GetStored();
            var stored3 = event3.GetStored();

            _store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.Open))
                .Returns(_stream.Object);
            _serializer.Setup(s => s.Deserialize(stored1.Data)).Returns(message1);
            _serializer.Setup(s => s.Deserialize(stored2.Data)).Returns(message2);
            _stream.Setup(s => s.GetSnapshot()).Returns((EventStoreSnapshot)null);
            _stream.Setup(s => s.GetEvents(1)).Returns(new[] { stored1, stored2 });

            _store.Setup(s => s.GetClock()).Returns(100).Verifiable();
            _store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.OpenExisting))
                .Returns(_stream.Object);
            _serializer.Setup(s => s.Serialize(message3)).Returns(stored3.Data).Verifiable();
            _stream.Setup(s => s.SaveEvents(2, It.Is<EventStoreEvent[]>(ea => ea.Single().Equals(stored3)))).Verifiable();
            _bus.Setup(b => b.Publish(message3)).Verifiable();
            _store.Setup(s => s.MarkAsPublished(stored3)).Verifiable();
            _factory.Setup(f => f.CreateMessage(event3, context, 100, 3)).Returns(message3).Verifiable();

            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            var aggregate = repository.Get(584);
            aggregate.PublishEvents(new[] { event3 });
            repository.Save(aggregate, context, RepositorySaveFlags.Append.ToVersion(2).WithoutSnapshot);

            _repo.Verify();
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
            var stored1 = event1.GetStored();
            var stored2 = event2.GetStored();
            var stored3 = event3.GetStored();
            var snapshot = new TestSnapshot() { Aggregate = 584, Version = 3 };
            var storedSnapshot = snapshot.GetStored();
            snapshot.Historic.AddRange(new[] { event1, event2, event3 });

            _store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.Open))
                .Returns(_stream.Object);
            _serializer.Setup(s => s.Deserialize(stored1.Data)).Returns(message1);
            _serializer.Setup(s => s.Deserialize(stored2.Data)).Returns(message2);
            _stream.Setup(s => s.GetSnapshot()).Returns((EventStoreSnapshot)null);
            _stream.Setup(s => s.GetEvents(1)).Returns(new[] { stored1, stored2 });

            _store.Setup(s => s.GetClock()).Returns(100).Verifiable();
            _store
                .Setup(s => s.GetStream("TestAggregate:584", EventStreamOpenMode.OpenExisting))
                .Returns(_stream.Object);
            _serializer.Setup(s => s.Serialize(message3)).Returns(stored3.Data).Verifiable();
            _serializer.Setup(s => s.Serialize(It.Is<Message>(m => m.Payload == snapshot))).Returns(storedSnapshot.Data).Verifiable();
            _stream
                .Setup(s => s.SaveEvents(
                    It.Is<int>(i => i == -1 || i == 2),
                    It.Is<EventStoreEvent[]>(ea => ea.Single().Equals(stored3))
                    )).Verifiable();
            _bus.Setup(b => b.Publish(message3)).Verifiable();
            _store.Setup(s => s.MarkAsPublished(stored3)).Verifiable();
            _stream.Setup(s => s.SaveSnapshot(It.IsAny<EventStoreSnapshot>()))
                .Callback<EventStoreSnapshot>(s => AssertExtension.AreEqual(storedSnapshot, s))
                .Verifiable();
            _factory.Setup(f => f.CreateMessage(event3, context, 100, 3)).Returns(message3).Verifiable();

            var repository = new TestRepository(_store.Object, _bus.Object, _factory.Object, _serializer.Object);
            var aggregate = repository.Get(584);
            aggregate.PublishEvents(new[] { event3 });
            aggregate.Snapshot = snapshot;
            repository.Save(aggregate, context, RepositorySaveFlags.Append.WithSnapshot);

            _repo.Verify();
            CollectionAssert.AreEqual(new[] { event1, event2, event3 }, aggregate.HistoricEvents);
            CollectionAssert.AreEqual(new IEvent[0], aggregate.NewEvents);
            Assert.AreEqual(3, aggregate.Version);
        }
    }
}
