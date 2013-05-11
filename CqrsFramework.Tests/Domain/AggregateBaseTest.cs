using System;
using CqrsFramework;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;

namespace CqrsFramework.Tests
{
    [TestClass]
    public class AggregateBaseTest
    {
        [TestMethod]
        public void ImplementsIAggregate()
        {
            var agg = new ImplementsIAggregate_TestAggregate();
            Assert.IsInstanceOfType(agg, typeof(IAggregate));
        }

        private class ImplementsIAggregate_TestAggregate : AggregateBase
        {
        }

        [TestMethod]
        public void CanRegisterEventHandlers()
        {
            var agg = new CanRegisterEventHandlers_TestAggregate();
        }

        private class CanRegisterEventHandlers_TestAggregate : AggregateBase
        {
            public CanRegisterEventHandlers_TestAggregate()
            {
                Register<TestEvent1>(Handle);
            }

            private void Handle(TestEvent1 e)
            {
            }
        }

        [TestMethod]
        public void CallsRegisteredHandlersOnLoading()
        {
            var agg = new ComplexTestAggregate();
            var event1 = new TestEvent1(5487);
            var event2 = new TestEvent2(2241, "Hello");
            var history = new IEvent[] { event1, event2 };
            ((IAggregate)agg).LoadFromHistory((object)null, (IEnumerable<IEvent>)history);
            CollectionAssert.AreEqual(history, agg.LoadedEvents);
        }

        private class ComplexTestAggregate : AggregateBase
        {
            public List<IEvent> LoadedEvents = new List<IEvent>();

            public ComplexTestAggregate()
            {
                Register<TestEvent1>(Handle);
                Register<TestEvent2>(Handle);
            }

            private void Handle(TestEvent1 e)
            {
                LoadedEvents.Add(e);
            }

            private void Handle(TestEvent2 e)
            {
                LoadedEvents.Add(e);
            }

            public new void Publish(IEvent @event)
            {
                base.Publish(@event);
            }
        }

        [TestMethod]
        public void CallsRegisteredHandlersOnPublish()
        {
            var agg = new ComplexTestAggregate();
            var event1 = new TestEvent1(8871);
            var event2 = new TestEvent2(3484, "Help me");
            var history = new IEvent[] { event1, event2 };
            agg.Publish(event1);
            agg.Publish(event2);
            CollectionAssert.AreEqual(history, agg.LoadedEvents);
        }

        [TestMethod]
        public void ReturnsEmptyChangesIfOnFreshlyLoadedAggregate()
        {
            var agg = new ComplexTestAggregate();
            var event1 = new TestEvent1(5487);
            var event2 = new TestEvent2(2241, "Hello");
            var history = new IEvent[] { event1, event2 };
            var iagg = (IAggregate)agg;
            iagg.LoadFromHistory((object)null, (IEnumerable<IEvent>)history);
            IEnumerable<IEvent> events = iagg.GetEvents();
            CollectionAssert.AreEqual(new IEvent[0], events.ToList());
        }

        [TestMethod]
        public void ReturnsPublishedEventsAsChanges()
        {
            var agg = new ComplexTestAggregate();
            var event1 = new TestEvent1(5487);
            var event2 = new TestEvent2(2241, "Hello");
            var history = new IEvent[] { event1, event2 };
            var iagg = (IAggregate)agg;
            agg.Publish(event1);
            agg.Publish(event2);
            IEnumerable<IEvent> events = iagg.GetEvents();
            CollectionAssert.AreEqual(history, events.ToList());
        }

        [TestMethod]
        public void CanCommitChangesToAggregate()
        {
            var agg = new ComplexTestAggregate();
            var event1 = new TestEvent1(5487);
            var event2 = new TestEvent2(2241, "Hello");
            var history = new IEvent[] { event1, event2 };
            var iagg = (IAggregate)agg;
            agg.Publish(event1);
            agg.Publish(event2);
            iagg.Commit();
            CollectionAssert.AreEqual(new IEvent[0], iagg.GetEvents().ToList());
        }

        [TestMethod]
        public void CanReturnSnapshot()
        {
            var agg = new SnapshotTestAggregate();
            agg.IntData = 5672;
            agg.StrData = "Hi";
            var iagg = (IAggregate)agg;
            var snapshot = (SnapshotTestAggregateSnapshot)iagg.GetSnapshot();
            Assert.IsNotNull(snapshot);
            Assert.AreEqual(5672, snapshot.IntData);
            Assert.AreEqual("Hi", snapshot.StrData);
        }

        private class SnapshotTestAggregateSnapshot
        {
            public int IntData;
            public string StrData;
        }

        private class SnapshotTestAggregate : AggregateBase
        {
            public int IntData;
            public string StrData;

            protected override object BuildSnapshot(object snapshot)
            {
                return new SnapshotTestAggregateSnapshot { IntData = IntData, StrData = StrData };
            }

            protected override void LoadSnapshot(object snapshotObj)
            {
                var snapshot = (SnapshotTestAggregateSnapshot)snapshotObj;
                IntData = snapshot.IntData;
                StrData = snapshot.StrData;
            }
        }
        
        [TestMethod]
        public void CanAcceptSnapshot()
        {
            var agg = new SnapshotTestAggregate();
            var iagg = (IAggregate)agg;
            var snapshot = new SnapshotTestAggregateSnapshot { IntData = 2312, StrData = "Help me" };
            iagg.LoadFromHistory(snapshot, new IEvent[0]);
            Assert.AreEqual(2312, agg.IntData);
            Assert.AreEqual("Help me", agg.StrData);
        }

        private class TestEvent1 : IEvent
        {
            public readonly int Data;

            public TestEvent1(int data)
            {
                this.Data = data;
            }
        }

        private class TestEvent2 : IEvent
        {
            public readonly int IntData;
            public readonly string StringData;

            public TestEvent2(int intData, string stringData)
            {
                this.IntData = intData;
                this.StringData = stringData;
            }
        }
    }
}
