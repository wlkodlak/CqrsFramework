using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.ServiceBus;
using CqrsFramework.Messaging;
using Moq;
using CqrsFramework.Infrastructure;
using System.Threading;

namespace CqrsFramework.Tests.ServiceBus
{
    [TestClass]
    public class HashsetMessageDeduplicatorTest
    {
        private HashsetMessageDeduplicator _dup;
        private TestTimeProvider _time;
        private List<Message> _allMessages;

        [TestInitialize]
        public void Initialize()
        {
            _time = new TestTimeProvider(new DateTime(2013, 6, 5, 14, 20, 33));
            _dup = new HashsetMessageDeduplicator(_time);
            _allMessages = Enumerable.Range(1, 500).Select(i => CreateMessage(i)).ToList();
        }

        private Message CreateMessage(int i)
        {
            var guid = Guid.NewGuid();
            var msg = new Message(string.Format("{0}: {1}", i, guid));
            msg.Headers.MessageId = guid;
            return msg;
        }

        private void HandleMessages(int skip, int count = 100)
        {
            foreach (var msg in _allMessages.Skip(skip).Take(count))
            {
                Assert.IsFalse(_dup.IsDuplicate(msg), "{0}", msg.Payload);
                _dup.MarkHandled(msg);
            }
        }
        private void VerifyDuplicates(int skip, int count)
        {
            foreach (var msg in _allMessages.Skip(skip).Take(count))
            {
                Assert.IsTrue(_dup.IsDuplicate(msg));
            }
        }

        [TestMethod]
        public void WithoutTimeChange()
        {
            HandleMessages(0);
            VerifyDuplicates(0, 4);
        }

        [TestMethod]
        public void WithTimeChange()
        {
            HandleMessages(0);
            VerifyDuplicates(0, 4);
            _time.AdvanceTime(TimeSpan.FromSeconds(30));
            HandleMessages(100);
            VerifyDuplicates(100, 4);
            VerifyDuplicates(0, 4);
        }

        [TestMethod]
        public void ForgetWithNoEffect()
        {
            HandleMessages(0);
            _time.AdvanceTime(TimeSpan.FromSeconds(25));
            HandleMessages(100);
            _time.AdvanceTime(TimeSpan.FromSeconds(25));
            _dup.ForgetOld(TimeSpan.FromSeconds(60));
            HandleMessages(200);
            VerifyDuplicates(0, 4);
        }

        [TestMethod]
        public void ForgetOldMessagesAndTryToReprocessThem()
        {
            HandleMessages(0);
            _time.AdvanceTime(TimeSpan.FromSeconds(35));
            HandleMessages(100);
            _time.AdvanceTime(TimeSpan.FromSeconds(35));
            _dup.ForgetOld(TimeSpan.FromSeconds(60));
            HandleMessages(200);
            HandleMessages(0, 4);
        }

        [TestMethod]
        public void AutoForget()
        {
            var cancel = new CancellationTokenSource();
            _dup.SetupAutoForget(cancel.Token, TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(60));
            HandleMessages(0);
            VerifyDuplicates(0, 4);
            _time.AdvanceTime(TimeSpan.FromSeconds(20));
            HandleMessages(100);
            VerifyDuplicates(0, 4);
            _time.AdvanceTime(TimeSpan.FromSeconds(20));
            HandleMessages(200);
            _time.AdvanceTime(TimeSpan.FromSeconds(20));
            VerifyDuplicates(100, 4);
            HandleMessages(300);
            _time.AdvanceTime(TimeSpan.FromSeconds(20));
            HandleMessages(0, 4);
        }
    }
}
