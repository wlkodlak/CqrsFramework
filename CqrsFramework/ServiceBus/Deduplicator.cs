using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CqrsFramework.Messaging;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.ServiceBus
{
    public interface IMessageDeduplicator
    {
        bool IsDuplicate(Message message);
        void MarkHandled(Message message);
    }

    public class InactiveMessageDeduplicator : IMessageDeduplicator
    {
        public bool IsDuplicate(Message message)
        {
            return false;
        }

        public void MarkHandled(Message message)
        {
        }
    }

    public class HashsetMessageDeduplicator : IMessageDeduplicator
    {
        private ITimeProvider _time;
        private ConcurrentDictionary<Guid, DateTime> _data;
        private Task _autoTask;

        public HashsetMessageDeduplicator(ITimeProvider time)
        {
            _data = new ConcurrentDictionary<Guid, DateTime>();
            _time = time;
        }

        public HashsetMessageDeduplicator SetupAutoForget(CancellationToken token, TimeSpan interval, TimeSpan threshold)
        {
            _autoTask = AutoForgetAsync(token, interval, threshold);
            return this;
        }

        private static Guid MessageId(Message message)
        {
            return message.Headers.MessageId;
        }

        private async Task AutoForgetAsync(CancellationToken token, TimeSpan interval, TimeSpan threshold)
        {
            while (!token.IsCancellationRequested)
            {
                await _time.WaitUntil(_time.Get() + interval, token);
                ForgetOld(threshold);
            }
        }

        public bool IsDuplicate(Message message)
        {
            return _data.ContainsKey(MessageId(message));
        }

        public void MarkHandled(Message message)
        {
            _data.TryAdd(MessageId(message), _time.Get());
        }

        public void ForgetOld(TimeSpan threshold)
        {
            var minimumTime = _time.Get() - threshold;
            var keys = _data.Where(p => p.Value < minimumTime).Select(p => p.Key).ToList();
            foreach (var key in keys)
            {
                DateTime time;
                _data.TryRemove(key, out time);
            }
        }
    }
}
