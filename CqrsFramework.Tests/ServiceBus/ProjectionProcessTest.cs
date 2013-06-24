using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using CqrsFramework.EventStore;
using CqrsFramework.ServiceBus;
using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;

namespace CqrsFramework.Tests.ServiceBus
{
    [TestClass]
    public class ProjectionProcessTest
    {
        private MockRepository _repo;
        private Mock<IEventStoreReader> _storeMock;
        private Mock<IMessageErrorPolicy> _errorMock;
        private Mock<IProjectionDispatcher> _projection1Mock;
        private Mock<IProjectionDispatcher> _projection2Mock;
        private Mock<IMessageSerializer> _serializerMock;

        private TestTimeProvider _time;
        private CancellationTokenSource _cancel;
        private TestStore _store;
        private TestProjection _projection1, _projection2;

        private ProjectionProcess _process;

        [TestInitialize]
        public void Initialize()
        {
            _repo = new MockRepository(MockBehavior.Strict);
            
            _storeMock = _repo.Create<IEventStoreReader>();
            _storeMock.Setup(s => s.Dispose()).Verifiable();

            _errorMock = _repo.Create<IMessageErrorPolicy>();
            _projection1Mock = _repo.Create<IProjectionDispatcher>();
            _projection2Mock = _repo.Create<IProjectionDispatcher>();
            _serializerMock = _repo.Create<IMessageSerializer>();

            _cancel = new CancellationTokenSource();
            _time = new TestTimeProvider(new DateTime(2013, 5, 21, 14, 11, 24));
            _store = new TestStore();
            _projection1 = new TestProjection();
            _projection2 = new TestProjection();
        }

        [TestMethod]
        public void SetupProcess()
        {
            var process = new ProjectionProcess(_storeMock.Object, _cancel.Token, _time, _serializerMock.Object);
            process.WithInterval(TimeSpan.FromMilliseconds(50));
            process.WithBlockSize(500);
            process.WithErrorPolicy(_errorMock.Object);
            process.Dispose();
            _repo.Verify();
        }

        [TestMethod]
        public void RegisterProjections()
        {
            _projection1Mock.Setup(p => p.NeedsRebuild()).Returns(false).Verifiable();
            _projection1Mock.Setup(p => p.GetClockToHandle()).Returns(1427).Verifiable();
            _projection2Mock.Setup(p => p.NeedsRebuild()).Returns(true).Verifiable();
            _projection2Mock.Setup(p => p.GetClockToHandle()).Returns(0);
            var process = new ProjectionProcess(_storeMock.Object, _cancel.Token, _time, _serializerMock.Object);
            process.Register(_projection1Mock.Object);
            process.Register(_projection2Mock.Object);
            process.Dispose();
            _repo.Verify();
        }

        [TestMethod]
        public void NoActionOnEmptyInput()
        {
            _projection1Mock.Setup(p => p.NeedsRebuild()).Returns(false).Verifiable();
            _projection1Mock.Setup(p => p.GetClockToHandle()).Returns(0).Verifiable();
            _projection2Mock.Setup(p => p.NeedsRebuild()).Returns(false).Verifiable();
            _projection2Mock.Setup(p => p.GetClockToHandle()).Returns(0).Verifiable();
            _storeMock.Setup(s => s.GetSince(0, 3)).Returns(new EventStoreEvent[0]).Verifiable();
            var process = new ProjectionProcess(_storeMock.Object, _cancel.Token, _time, _serializerMock.Object).WithBlockSize(3);
            process.Register(_projection1Mock.Object);
            process.Register(_projection2Mock.Object);
            Task task = process.ProcessNext();
            Assert.IsFalse(task.IsCompleted, "Should wait");
            _cancel.Cancel();
            process.Dispose();
            _repo.Verify();
        }

        [TestMethod]
        public void NoActionOnCurrentProjections()
        {
            for (int i = 0; i < 6; i++)
                _store.Add();
            _store.CurrentPosition = 6;
            _projection1.SetupClock = 6;
            _projection2.SetupClock = 6;

            _storeMock.Setup(s => s.GetSince(6, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            PrepareTest();
            WaitsForCancel();
            FinishTest();
        }

        [TestMethod]
        public void ReceiveNewEventsOnCurrentProjections()
        {
            for (int i = 0; i < 10; i++)
                _store.Add();
            _store.CurrentPosition = 8;
            _projection1.SetupClock = 8;
            _projection2.SetupClock = 8;

            _storeMock.Setup(s => s.GetSince(8, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            PrepareTest();
            ReceiveEvents(8, 2, 2);
            ExpectMessages(_projection1, 8, 2, "Projection1");
            ExpectMessages(_projection2, 8, 2, "Projection2");
            FinishTest();
        }

        [TestMethod]
        public void UpdatingOneProjectionDoesNotRedundantlyUpdateSecondOne()
        {
            for (int i = 0; i < 10; i++)
                _store.Add();
            _store.CurrentPosition = 8;
            _projection1.SetupClock = 4;
            _projection2.SetupClock = 6;
            
            _storeMock.Setup(s => s.GetSince(4, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            _storeMock.Setup(s => s.GetSince(7, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            _storeMock.Setup(s => s.GetSince(8, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            PrepareTest();
            ReceiveEvents(4, 6, 2);
            ExpectMessages(_projection1, 4, 6, "Projection1");
            ExpectMessages(_projection2, 6, 4, "Projection2");
            FinishTest();
        }

        [TestMethod]
        public void ResetOneProjectionUpdateTheOther()
        {
            for (int i = 0; i < 8; i++)
                _store.Add();
            _store.CurrentPosition = 8;
            _projection1.SetupClock = 4;
            _projection2.SetupRebuild = true;
            _projection2.SetupClock = 6;
            
            _storeMock.Setup(s => s.GetSince(0, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            _storeMock.Setup(s => s.GetSince(3, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            _storeMock.Setup(s => s.GetSince(6, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            _storeMock.Setup(s => s.GetSince(8, 3)).Returns<long, int>(_store.GetSince).Verifiable();
            PrepareTest();
            ReceiveEvents(0, 8, 0);
            WaitsForCancel();
            ExpectMessages(_projection1, 4, 4, "Projection1");
            ExpectMessages(_projection2, 0, 8, "Projection2");
            FinishTest();
        }

        [TestMethod, Ignore]
        public void SuccessAfterRetry()
        {
            
        }

        [TestMethod, Ignore]
        public void OneProjectionFailsOtherCompletesAndGetsNewMessages()
        {
            
        }

        private void PrepareTest()
        {
            _serializerMock.Setup(s => s.Serialize(It.IsAny<Message>())).Returns<Message>(Serialize);
            _serializerMock.Setup(s => s.Deserialize(It.IsAny<byte[]>())).Returns<byte[]>(Deserialize);

            _projection1Mock.Setup(p => p.BeginUpdate()).Callback(_projection1.BeginUpdate);
            _projection1Mock.Setup(p => p.Dispatch(It.IsAny<Message>())).Callback<Message>(_projection1.Dispatch);
            _projection1Mock.Setup(p => p.EndUpdate()).Callback(_projection1.EndUpdate);
            _projection1Mock.Setup(p => p.GetClockToHandle()).Returns(_projection1.GetClockToHandle);
            _projection1Mock.Setup(p => p.NeedsRebuild()).Returns(_projection1.NeedsRebuild).Verifiable();
            _projection1Mock.Setup(p => p.Reset()).Callback(_projection1.Reset);
            
            _projection2Mock.Setup(p => p.BeginUpdate()).Callback(_projection2.BeginUpdate);
            _projection2Mock.Setup(p => p.Dispatch(It.IsAny<Message>())).Callback<Message>(_projection2.Dispatch);
            _projection2Mock.Setup(p => p.EndUpdate()).Callback(_projection2.EndUpdate);
            _projection2Mock.Setup(p => p.GetClockToHandle()).Returns(_projection2.GetClockToHandle);
            _projection2Mock.Setup(p => p.NeedsRebuild()).Returns(_projection2.NeedsRebuild).Verifiable();
            _projection2Mock.Setup(p => p.Reset()).Callback(_projection2.Reset);

            _process = new ProjectionProcess(_storeMock.Object, _cancel.Token, _time, _serializerMock.Object).WithBlockSize(3);
            _process.Register(_projection1Mock.Object);
            _process.Register(_projection2Mock.Object);
        }

        private void FinishTest()
        {
            _process.Dispose();
            _repo.Verify();
        }

        private void WaitsForCancel()
        {
            var task = _process.ProcessNext();
            Assert.IsFalse(task.IsCompleted);
            _time.ChangeTime(_time.Get().AddMinutes(30));
            Assert.IsFalse(task.IsCompleted);
            _cancel.Cancel();
            Assert.IsTrue(task.IsCanceled);
        }

        private void ReceiveEvents(int offset, int stepsCount, int storeMoves)
        {
            while (stepsCount > 0)
            {
                var task = _process.ProcessNext();
                if (!task.IsCompleted)
                {
                    if (storeMoves > 0)
                    {
                        _store.CurrentPosition += storeMoves;
                        storeMoves = 0;
                    }
                    var newTime = _time.Get().AddMilliseconds(200);
                    _time.ChangeTime(newTime);
                }
                Assert.IsTrue(task.IsCompleted, "Offset {0}: task didn't complete in time", offset);
                offset++;
                stepsCount--;
            }
        }

        private void ExpectMessages(TestProjection projection, int clock, int count, string note)
        {
            var expectedStored = _store.AllEvents.Where(e => e.Clock >= clock).Take(count).ToList();
            var expectedMessages = expectedStored.Select(e => Deserialize(e.Data)).ToList();
            var actualMessages = projection.Contents.ToList();
            if (expectedStored.Count == 0)
            {
                Assert.IsTrue(actualMessages.Count == 0, "{0}: Expected empty", note);
                return;
            }
            Assert.AreEqual(projection.SetupClock, expectedStored.Max(e => e.Clock + 1), "{0}: end clock", note);
            Assert.IsFalse(projection.IsInUpdate, "{0}: in update", note);
            for (int i = 0; i < Math.Min(expectedStored.Count, actualMessages.Count); i++)
            {
                Assert.AreEqual(expectedMessages[i].Payload, actualMessages[i].Payload, "{0}: payload {1}", note, i);
                Assert.AreEqual(expectedMessages[i].Headers.MessageId, actualMessages[i].Headers.MessageId, "{0}: message id {1}", note, i);
            }
            Assert.AreEqual(expectedMessages.Count, actualMessages.Count, "{0}: Count differs", note);
        }

        private static byte[] Serialize(Message msg)
        {
            var xelem = new XElement("Message",
                msg.Headers.Select(h => new XAttribute(h.Name, h.Value)),
                new XText(msg.Payload.ToString()));
            return new UTF8Encoding(false).GetBytes(xelem.ToString());
        }

        private static Message Deserialize(byte[] bytes)
        {
            var xelem = XElement.Parse(new UTF8Encoding(false).GetString(bytes));
            var msg = new Message(xelem.Value);
            foreach (var attr in xelem.Attributes())
                msg.Headers[attr.Name.LocalName] = attr.Value;
            return msg;
        }

        private class TestStore
        {
            private Random _rand = new Random(5472);
            public int Clock = 0;
            public long CurrentPosition = 0;
            public List<EventStoreEvent> AllEvents = new List<EventStoreEvent>();
            private int[] _versions = new int[4] { 0, 0, 0, 0 };
            private string[] _names = new string[4] { "a", "b", "c", "d" };

            public void Add()
            {
                int aggregate = _rand.Next(4);
                _versions[aggregate]++;
                var msg = new Message(string.Format("{0}:{1}", _names[aggregate], _versions[aggregate]));
                msg.Headers.MessageId = Guid.NewGuid();
                AllEvents.Add(new EventStoreEvent
                {
                    Clock = Clock++,
                    Key = _names[aggregate],
                    Version = _versions[aggregate],
                    Published = true,
                    Data = Serialize(msg)
                });
            }

            public IEnumerable<EventStoreEvent> GetSince(long clock, int maxCount)
            {
                return AllEvents.Where(e => e.Clock >= clock && e.Clock < CurrentPosition).Take(maxCount).ToList();
            }
        }

        private class TestProjection : IProjectionDispatcher
        {
            public bool SetupRebuild;
            public long SetupClock;
            public bool IsInUpdate;
            public List<Message> Contents = new List<Message>();

            public void BeginUpdate()
            {
                IsInUpdate = true;
            }

            public void EndUpdate()
            {
                IsInUpdate = false;
            }

            public void Reset()
            {
                if (SetupRebuild)
                {
                    SetupRebuild = false;
                    SetupClock = 0;
                    Contents.Clear();
                }
                else
                    throw new InvalidOperationException("Rebuild not expected");
            }

            public bool NeedsRebuild()
            {
                return SetupRebuild;
            }

            public long GetClockToHandle()
            {
                return SetupClock;
            }

            public void Dispatch(Message message)
            {
                Contents.Add(message);
                SetupClock = message.Headers.EventClock + 1;
            }
        }
    }
}
