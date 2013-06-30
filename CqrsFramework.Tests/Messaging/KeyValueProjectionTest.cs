using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Messaging;
using Moq;
using CqrsFramework.Serialization;
using System.Xml.Linq;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class KeyValueProjectionTest
    {
        #region Helpers
        private static Encoding _utf8 = new UTF8Encoding(false);

        private interface ITestEvent
        {
            int AggregateId { get; }
        }

        private class TestEvent1 : ITestEvent
        {
            public int AggregateId { get; set; }
        }

        private class TestEvent2 : ITestEvent
        {
            public int AggregateId { get; set; }
        }

        private class TestView
        {
            public TestView()
            {
            }

            public List<object> Events = new List<object>();

            public TestView WithEvent(TestEvent1 ev)
            {
                Events.Add(ev);
                return this;
            }

            public TestView WithEvent(TestEvent2 ev)
            {
                Events.Add(ev);
                return this;
            }

            public TestView WithMany(IEnumerable<Message> messages)
            {
                Events.AddRange(messages.Select(m => m.Payload));
                return this;
            }
        }

        private class KeyValueInfo
        {
            public string Key;
            public int InitialVersion;
            public byte[] InitialData;
            public int CurrentVersion;
            public byte[] CurrentData;
            public int ExpectedVersion;
            public byte[] ExpectedData;
            public TestView ExpectedDocument;
        }

        private class ProjectionInfo
        {
            public int ClockVersion;
            public long InitialClock;
            public long CurrentClock;
            public long ExpectedClock = -1;
            public bool InitialRebuild;
            public byte[] CurrentHash;
            public Dictionary<string, KeyValueInfo> Documents = new Dictionary<string, KeyValueInfo>();
            public List<Message> BulkEvents = new List<Message>();
            public List<Message> IndividualEvents = new List<Message>();
            public Action<KeyValueProjection<TestView>> RegisterHandlers;
            public bool ExpectedFailure;
            public bool IsInBulkMode;
            public bool AllowSavingInBulkMode = false;
        }

        private class TestBuilder
        {
            private ProjectionInfo _info;
            private Mock<IKeyValueStore> _store;
            private Mock<IKeyValueProjectionStrategy> _strategy;

            public TestBuilder()
            {
                _info = new ProjectionInfo();
                _info.CurrentHash = Encoding.ASCII.GetBytes("OK");

                _store = new Mock<IKeyValueStore>(MockBehavior.Strict);
                _store.Setup(s => s.Purge()).Callback(PurgeStore);
                _store.Setup(s => s.Get(It.IsAny<string>())).Returns<string>(GetDocument);
                _store.Setup(s => s.Set(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<byte[]>())).Returns<string, int, byte[]>(SetDocument);

                _strategy = new Mock<IKeyValueProjectionStrategy>();
                _strategy
                    .Setup(s => s.SerializeView(typeof(TestView), It.IsAny<TestView>()))
                    .Returns<Type, object>((t, o) => SerializeView(o));
                _strategy
                    .Setup(s => s.DeserializeView(typeof(TestView), It.IsAny<byte[]>()))
                    .Returns<Type, byte[]>((t, b) => DeserializeView(b));
                _strategy.Setup(s => s.GetTypename(typeof(TestView))).Returns("TestViewTypeName");
                _strategy.Setup(s => s.SerializeClock(It.IsAny<long>())).Returns<long>(ByteArrayUtils.TextLong);
                _strategy.Setup(s => s.DeserializeClock(It.IsAny<byte[]>())).Returns<byte[]>(ByteArrayUtils.TextLong);
            }

            public TestBuilder WithRebuild()
            {
                _info.InitialRebuild = true;
                _info.CurrentHash = Encoding.ASCII.GetBytes("Rebuild");
                return this;
            }

            public TestBuilder WithClock(long clock)
            {
                _info.ClockVersion = 31;
                _info.InitialClock = _info.CurrentClock = clock;
                return this;
            }

            public TestBuilder ExpectClock(long clock)
            {
                _info.ExpectedClock = clock;
                return this;
            }

            public TestBuilder WithDocument(string key, int version, TestView document)
            {
                KeyValueInfo docInfo;
                if (!_info.Documents.TryGetValue(key, out docInfo))
                {
                    _info.Documents[key] = docInfo = new KeyValueInfo();
                    docInfo.Key = key;
                }
                docInfo.InitialVersion = docInfo.CurrentVersion = version;
                var serializationHeaders = new MessageHeaders();
                docInfo.InitialData = document == null ? null : SerializeView(document);
                docInfo.CurrentData = docInfo.InitialData;
                return this;
            }

            public TestBuilder ExpectDocument(string key, int version, TestView document)
            {
                KeyValueInfo docInfo;
                if (!_info.Documents.TryGetValue(key, out docInfo))
                {
                    _info.Documents[key] = docInfo = new KeyValueInfo();
                    docInfo.Key = key;
                }
                docInfo.ExpectedVersion = version;
                docInfo.ExpectedDocument = document;
                var serializationHeaders = new MessageHeaders();
                docInfo.ExpectedData = document == null ? null : SerializeView(document);
                return this;
            }

            public void ExpectFailure()
            {
                _info.ExpectedFailure = true;
            }

            public TestBuilder WithRegistrations(Action<KeyValueProjection<TestView>> registrator)
            {
                _info.RegisterHandlers = registrator;
                return this;
            }

            public TestBuilder SendBulk(IEnumerable<Message> messages)
            {
                _info.BulkEvents.AddRange(messages);
                return this;
            }

            public TestBuilder SendBulk(params Message[] messages)
            {
                _info.BulkEvents.AddRange(messages);
                return this;
            }

            public TestBuilder SendIndividual(IEnumerable<Message> messages)
            {
                _info.IndividualEvents.AddRange(messages);
                return this;
            }

            public TestBuilder SendIndividual(params Message[] messages)
            {
                _info.IndividualEvents.AddRange(messages);
                return this;
            }

            public void RunTest()
            {
                var projection = new KeyValueProjection<TestView>(_store.Object,
                    _strategy.Object, "TestProjection", Encoding.ASCII.GetBytes("OK"));
                if (_info.RegisterHandlers != null)
                    _info.RegisterHandlers(projection);
                var rebuildFromProjection = projection.NeedsRebuild();
                var firstClock = projection.GetClockToHandle();
                Assert.AreEqual(_info.InitialRebuild, rebuildFromProjection, "Rebuild");
                Assert.AreEqual(_info.InitialRebuild ? 0 : _info.InitialClock, firstClock, "Clock start");
                bool dispatchFailed = false;

                try
                {
                    if (_info.BulkEvents.Count > 0)
                    {
                        projection.BeginUpdate();
                        try
                        {
                            _info.IsInBulkMode = true;
                            if (rebuildFromProjection)
                                projection.Reset();
                            foreach (var ev in _info.BulkEvents)
                                projection.Dispatch(ev);
                        }
                        finally
                        {
                            _info.IsInBulkMode = false;
                            projection.EndUpdate();
                        }
                    }
                    foreach (var ev in _info.IndividualEvents)
                        projection.Dispatch(ev);
                }
                catch (InvalidOperationException)
                {
                    if (!_info.ExpectedFailure)
                        throw;
                    else
                        dispatchFailed = true;
                }

                foreach (var doc in _info.Documents.Values)
                {
                    var view = doc.CurrentData == null ? null : (TestView)DeserializeView(doc.CurrentData);
                    AssertExtension.AreEqual(doc.ExpectedDocument, view, string.Format("View {0}", doc.Key));
                    Assert.AreEqual(doc.ExpectedVersion, doc.CurrentVersion, "Version {0}", doc.Key);
                }

                if (_info.ExpectedClock != -1)
                    Assert.AreEqual(_info.ExpectedClock, _info.CurrentClock, "Ending clock");
                AssertExtension.AreEqual(Encoding.ASCII.GetBytes("OK"), _info.CurrentHash, "Hash");

                if (_info.ExpectedFailure && !dispatchFailed)
                    Assert.Fail("Expected projection failure");
            }

            private KeyValueDocument GetDocument(string key)
            {
                if (key == "TestProjection__Clock")
                    return new KeyValueDocument(key, _info.ClockVersion, ByteArrayUtils.TextLong(_info.CurrentClock));
                else if (key == "TestProjection__Hash")
                    return _info.CurrentHash == null ? null : new KeyValueDocument(key, _info.ClockVersion, _info.CurrentHash);

                KeyValueInfo docInfo;
                if (!_info.Documents.TryGetValue(key, out docInfo))
                    return null;
                else if (docInfo.CurrentData == null)
                    return null;
                else
                    return new KeyValueDocument(docInfo.Key, docInfo.CurrentVersion, docInfo.CurrentData);
            }

            private int SetDocument(string key, int expectedVersion, byte[] data)
            {
                if (_info.IsInBulkMode && !_info.AllowSavingInBulkMode)
                    Assert.Fail("Cannot save data during bulk mode: {0}", key);

                if (key == "TestProjection__Clock")
                {
                    _info.ClockVersion++;
                    _info.CurrentClock = ByteArrayUtils.TextLong(data);
                    return _info.ClockVersion;
                }
                else if (key == "TestProjection__Hash")
                {
                    _info.ClockVersion++;
                    _info.CurrentHash = data;
                    return _info.ClockVersion;
                }

                KeyValueInfo docInfo;
                if (!_info.Documents.TryGetValue(key, out docInfo))
                {
                    _info.Documents[key] = docInfo = new KeyValueInfo();
                    docInfo.Key = key;
                }
                docInfo.CurrentVersion++;
                docInfo.CurrentData = data;
                return docInfo.CurrentVersion;
            }

            private void PurgeStore()
            {
                Assert.IsTrue(_info.InitialRebuild, "Rebuild not requested");
                _info.CurrentHash = null;
                _info.CurrentClock = 0;
                foreach (var doc in _info.Documents.Values)
                {
                    doc.CurrentData = null;
                    doc.CurrentVersion = 0;
                }
            }
        }

        private Message CreateMessage(object @event, long clock)
        {
            var message = new Message(@event);
            message.Headers.MessageId = Guid.NewGuid();
            message.Headers.EventClock = clock;
            return message;
        }

        private static byte[] SerializeView(object payload)
        {
            var view = payload as TestView;
            var xelem = new XElement("TestView");
            foreach (var raw in view.Events)
            {
                if (raw is TestEvent1)
                {
                    var ev = raw as TestEvent1;
                    xelem.Add(new XElement("TestEvent1", new XAttribute("AggregateId", ev.AggregateId)));
                }
                else if (raw is TestEvent2)
                {
                    var ev = raw as TestEvent2;
                    xelem.Add(new XElement("TestEvent2", new XAttribute("AggregateId", ev.AggregateId)));
                }
            }
            return _utf8.GetBytes(xelem.ToString());
        }

        private static object DeserializeView(byte[] bytes)
        {
            var xelem = XElement.Parse(_utf8.GetString(bytes));
            var view = new TestView();
            foreach (var elem in xelem.Elements())
            {
                if (elem.Name.LocalName == "TestEvent1")
                    view.Events.Add(new TestEvent1 { AggregateId = (int)elem.Attribute("AggregateId") });
                else if (elem.Name.LocalName == "TestEvent2")
                    view.Events.Add(new TestEvent2 { AggregateId = (int)elem.Attribute("AggregateId") });
            }
            return view;
        }
        #endregion

        [TestMethod]
        public void RegisterHandler()
        {
            var store = new Mock<IKeyValueStore>();
            var projection = new KeyValueProjection<TestView>(store.Object, CreateStrategy().Object, "TestProjection", new byte[] { 5 });
            projection.Register<TestEvent1>(ev => ev.AggregateId.ToString(), (ev, h) => new TestView().WithEvent(ev), (ev, h, view) => view.WithEvent(ev));
            projection.Register<TestEvent2>(ev => ev.AggregateId.ToString(), (ev, h) => new TestView().WithEvent(ev), (ev, h, view) => view.WithEvent(ev));
        }

        [TestMethod]
        public void DetectThatItNeedsRebuild()
        {
            var store = new Mock<IKeyValueStore>();
            store.Setup(s => s.Get("TestProjection__Hash")).Returns(new KeyValueDocument("TestProjection__Hash", 1, Encoding.ASCII.GetBytes("Rebuild"))).Verifiable();
            var projection = new KeyValueProjection<TestView>(store.Object, CreateStrategy().Object, "TestProjection", Encoding.ASCII.GetBytes("OK"));
            Assert.IsTrue(projection.NeedsRebuild(), "Rebuild");
        }

        [TestMethod]
        public void DetectClock()
        {
            var store = new Mock<IKeyValueStore>();
            store.Setup(s => s.Get("TestProjection__Hash")).Returns(new KeyValueDocument("TestProjection__Hash", 1, Encoding.ASCII.GetBytes("OK"))).Verifiable();
            store.Setup(s => s.Get("TestProjection__Clock")).Returns(new KeyValueDocument("TestProjection__Clock", 472, ByteArrayUtils.TextLong(7184))).Verifiable();
            var projection = new KeyValueProjection<TestView>(store.Object, CreateStrategy().Object, "TestProjection", Encoding.ASCII.GetBytes("OK"));
            Assert.IsFalse(projection.NeedsRebuild(), "Rebuild");
            Assert.AreEqual(7184, projection.GetClockToHandle(), "Clock");
        }

        private Mock<IKeyValueProjectionStrategy> CreateStrategy()
        {
            var mock = new Mock<IKeyValueProjectionStrategy>();
            mock.Setup(s => s.GetTypename(typeof(TestView))).Returns("TestViewTypeName");
            mock.Setup(s => s.SerializeClock(It.IsAny<long>())).Returns<long>(ByteArrayUtils.TextLong);
            mock.Setup(s => s.DeserializeClock(It.IsAny<byte[]>())).Returns<byte[]>(ByteArrayUtils.TextLong);
            return mock;
        }

        [TestMethod]
        public void CreateIndividualDocument()
        {
            var ev1 = new TestEvent1 { AggregateId = 584 };
            var test = new TestBuilder();
            test.WithRegistrations(p => p.Register<TestEvent1>(
                e => "TestAggregate:" + e.AggregateId,
                (e, h) => new TestView().WithEvent(e), null));
            test.SendIndividual(CreateMessage(ev1, 48));
            test.ExpectDocument("TestAggregate:584", 1, new TestView().WithEvent(ev1));
            test.ExpectClock(49);
            test.RunTest();
        }

        [TestMethod]
        public void CreateIndividualDocumentUsingInterfaceHandler()
        {
            var ev1 = new TestEvent1 { AggregateId = 584 };
            var test = new TestBuilder();
            test.WithRegistrations(p => p.Register<ITestEvent>(
                e => "TestAggregate:" + e.AggregateId,
                (e, h) => new TestView().WithEvent((TestEvent1)e), null));
            test.SendIndividual(CreateMessage(ev1, 48));
            test.ExpectDocument("TestAggregate:584", 1, new TestView().WithEvent(ev1));
            test.ExpectClock(49);
            test.RunTest();
        }

        [TestMethod]
        public void UpdateIndividualDocument()
        {
            var ev1 = new TestEvent1 { AggregateId = 584 };
            var ev2 = new TestEvent2 { AggregateId = 584 };
            var test = new TestBuilder();
            var view = new TestView().WithEvent(ev1);
            test.WithDocument("TestAggregate:584", 1, view);
            test.WithRegistrations(p => p.Register<TestEvent2>(
                e => "TestAggregate:" + e.AggregateId, null,
                (e, h, v) => v.WithEvent(e)));
            test.SendIndividual(CreateMessage(ev2, 48));
            test.ExpectDocument("TestAggregate:584", 2, view.WithEvent(ev2));
            test.ExpectClock(49);
            test.RunTest();
        }

        [TestMethod]
        public void FailToCreateIndividualDocument()
        {
            var ev1 = new TestEvent1 { AggregateId = 584 };
            var test = new TestBuilder();
            var view = new TestView().WithEvent(ev1);
            test.WithDocument("TestAggregate:584", 1, view);
            test.WithRegistrations(p => p.Register<TestEvent1>(
                e => "TestAggregate:" + e.AggregateId,
                (e, h) => new TestView().WithEvent(e), null));
            test.SendIndividual(CreateMessage(ev1, 48));
            test.ExpectDocument("TestAggregate:584", 1, view);
            test.ExpectClock(0);
            test.ExpectFailure();
            test.RunTest();
        }

        [TestMethod]
        public void FailToUpdateIndividualDocument()
        {
            var ev2 = new TestEvent2 { AggregateId = 584 };
            var test = new TestBuilder();
            test.WithRegistrations(p => p.Register<TestEvent2>(
                e => "TestAggregate:" + e.AggregateId, null,
                (e, h, v) => v.WithEvent(e)));
            test.SendIndividual(CreateMessage(ev2, 48));
            test.ExpectDocument("TestAggregate:584", 0, null);
            test.ExpectClock(0);
            test.ExpectFailure();
            test.RunTest();
        }

        private IEnumerable<Message> GetAllEvents(int minClock, int maxClock, int aggregate)
        {
            var filtered = GetAllEvents().Where(m => m.Headers.EventClock >= minClock && m.Headers.EventClock <= maxClock);
            if (aggregate != 0)
                filtered = filtered.Where(m => (m.Payload as ITestEvent).AggregateId == aggregate);
            return filtered;
        }

        private IEnumerable<Message> GetAllEvents()
        {
            yield return CreateMessage(new TestEvent1 { AggregateId = 421 }, 0);
            yield return CreateMessage(new TestEvent2 { AggregateId = 421 }, 10);
            yield return CreateMessage(new TestEvent2 { AggregateId = 12 }, 20);
            yield return CreateMessage(new TestEvent1 { AggregateId = 584 }, 50);
            yield return CreateMessage(new TestEvent2 { AggregateId = 421 }, 60);
            yield return CreateMessage(new TestEvent1 { AggregateId = 584 }, 70);
            yield return CreateMessage(new TestEvent2 { AggregateId = 584 }, 80);
            yield return CreateMessage(new TestEvent1 { AggregateId = 421 }, 90);
            yield return CreateMessage(new TestEvent2 { AggregateId = 697 }, 100);
            yield return CreateMessage(new TestEvent1 { AggregateId = 697 }, 110);
            yield return CreateMessage(new TestEvent1 { AggregateId = 584 }, 120);
        }

        [TestMethod]
        public void SuccessfulUpdate()
        {
            var test = new TestBuilder();
            test.WithClock(50);
            test.WithRegistrations(FullRegistration);
            var viewB = new TestView().WithMany(GetAllEvents(0, 50, 421));
            test.WithDocument("TestAggregate:421", 47, viewB);
            test.SendBulk(GetAllEvents(50, 90, 0));
            test.ExpectClock(91);
            test.ExpectDocument("TestAggregate:421", 48, viewB.WithMany(GetAllEvents(50, 90, 421)));
            test.ExpectDocument("TestAggregate:584", 1, new TestView().WithMany(GetAllEvents(50, 90, 584)));
            test.RunTest();
        }

        [TestMethod]
        public void SuccessfulRebuild()
        {
            var test = new TestBuilder();
            test.WithClock(50);
            test.WithRegistrations(FullRegistration);
            test.WithRebuild();
            test.WithDocument("TestAggregate:421", 47, new TestView().WithMany(GetAllEvents(0, 50, 421)));
            test.WithDocument("TestAggregate:111", 38, new TestView()
                .WithEvent(new TestEvent1 { AggregateId = 111 })
                .WithEvent(new TestEvent2 { AggregateId = 111 }));
            test.SendBulk(GetAllEvents(0, 90, 0));
            test.ExpectClock(91);
            test.ExpectDocument("TestAggregate:584", 1, new TestView().WithMany(GetAllEvents(0, 90, 584)));
            test.ExpectDocument("TestAggregate:421", 1, new TestView().WithMany(GetAllEvents(0, 90, 421)));
            test.ExpectDocument("TestAggregate:697", 0, null);
            test.ExpectDocument("TestAggregate:12", 1, new TestView().WithMany(GetAllEvents(0, 90, 12)));
            test.ExpectDocument("TestAggregate:111", 0, null);
            test.RunTest();
        }

        [TestMethod]
        public void IndividualsAfterRebuild()
        {
            var test = new TestBuilder();
            test.WithClock(0);
            test.WithRegistrations(FullRegistration);
            test.WithRebuild();
            test.SendBulk(GetAllEvents(0, 90, 0));
            test.SendIndividual(GetAllEvents(100, 120, 0));
            test.ExpectClock(121);
            test.ExpectDocument("TestAggregate:584", 2, new TestView().WithMany(GetAllEvents(0, 120, 584)));
            test.ExpectDocument("TestAggregate:421", 1, new TestView().WithMany(GetAllEvents(0, 120, 421)));
            test.ExpectDocument("TestAggregate:697", 2, new TestView().WithMany(GetAllEvents(0, 120, 697)));
            test.ExpectDocument("TestAggregate:12", 1, new TestView().WithMany(GetAllEvents(0, 120, 12)));
            test.RunTest();
        }

        private static void FullRegistration(KeyValueProjection<TestView> p)
        {
            p.Register<TestEvent1>(
                e => "TestAggregate:" + e.AggregateId,
                e => new TestView().WithEvent(e),
                (e, v) => v.WithEvent(e));
            p.Register<TestEvent2>(
                e => "TestAggregate:" + e.AggregateId,
                (e, h) => new TestView().WithEvent(e),
                (e, h, v) => v.WithEvent(e));
        }

    }
}
