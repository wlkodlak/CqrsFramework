using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.ServiceBus
{
    public interface IMessageErrorPolicy
    {
        void HandleException(IMessageInboxReader inbox, Message message, Exception exception);
    }

    public class MessageErrorPolicy : IMessageErrorPolicy
    {
        private ITimeProvider _time;
        private Dictionary<Type, MessageErrorPolicySettings> _settings;

        public MessageErrorPolicy(ITimeProvider time)
        {
            _time = time;
            _settings = new Dictionary<Type, MessageErrorPolicySettings>();
        }

        public MessageErrorPolicySetup Default
        {
            get { return new MessageErrorPolicySetup(this, GetSettingsForSetup(typeof(Exception))); }
        }

        public MessageErrorPolicySetup For<T>() where T : Exception
        {
            return new MessageErrorPolicySetup(this, GetSettingsForSetup(typeof(T)));
        }

        public MessageErrorPolicySettings GetSettingsForSetup(Type type)
        {
            MessageErrorPolicySettings settings;
            if (!_settings.TryGetValue(type, out settings))
            {
                settings = FindSettings(type).CloneFor(type);
                _settings[type] = settings;
            }
            return settings;
        }

        private MessageErrorPolicySettings FindSettings(Type type)
        {
            MessageErrorPolicySettings settings;
            var searchedType = type;
            while (searchedType != typeof(object))
            {
                if (_settings.TryGetValue(searchedType, out settings))
                    return settings;
                if (searchedType == typeof(Exception))
                    break;
                else
                    searchedType = searchedType.BaseType;
            }
            return new MessageErrorPolicySettings(type);
        }

        public void HandleException(IMessageInboxReader inbox, Message message, Exception exception)
        {
            var settings = FindSettings(exception.GetType());
            var retryNumber = message.Headers.RetryNumber;
            if (retryNumber < settings.RetryCount)
            {
                int delay = ComputeDelay(retryNumber + 1, settings.DelayFactors);
                message.Headers.RetryNumber = retryNumber + 1;
                message.Headers.DeliverOn = _time.Get().AddMilliseconds(delay);
                inbox.Put(message);
            }
            else
            {
                if (settings.ErrorQueueWriter != null)
                    settings.ErrorQueueWriter.Put(message);
                inbox.Delete(message);
            }
        }

        private int ComputeDelay(int retryNumber, int[] factors)
        {
            int delay = factors[0] + factors[1] * retryNumber + factors[2] * retryNumber * retryNumber;
            for (int i = 3; i < factors.Length; i++)
                delay += factors[i] * IntPower(retryNumber, i);
            return delay;
        }

        private static int IntPower(int number, int power)
        {
            int result = 1;
            for (int i = 0; i < power; i++)
                result *= number;
            return result;
        }
    }

    public class MessageErrorPolicySettings
    {
        public Type Type;
        public int RetryCount;
        public int[] DelayFactors;
        public IMessageInboxWriter ErrorQueueWriter;

        public MessageErrorPolicySettings(Type type)
        {
            this.Type = type;
            this.DelayFactors = new int[0];
        }

        public MessageErrorPolicySettings CloneFor(Type type)
        {
            var copy = new MessageErrorPolicySettings(type);
            copy.RetryCount = this.RetryCount;
            copy.DelayFactors = this.DelayFactors;
            copy.ErrorQueueWriter = this.ErrorQueueWriter;
            return copy;
        }
    }

    public class MessageErrorPolicySetup
    {
        private MessageErrorPolicy _parent;
        private List<MessageErrorPolicySettings> _settings;

        public MessageErrorPolicySetup(MessageErrorPolicy parent, MessageErrorPolicySettings settings)
        {
            _parent = parent;
            _settings = new List<MessageErrorPolicySettings>();
            _settings.Add(settings);
        }

        public MessageErrorPolicySetup And<T>() where T : Exception
        {
            _settings.Add(_parent.GetSettingsForSetup(typeof(T)));
            return this;
        }

        public MessageErrorPolicySetup Retry(int count)
        {
            foreach (var settings in _settings)
                settings.RetryCount = count;
            return this;
        }

        public MessageErrorPolicySetup Delay(params int[] delayFactors)
        {
            int[] extended;
            if (delayFactors.Length == 3)
                extended = delayFactors;
            else
            {
                extended = new int[Math.Max(3, delayFactors.Length)];
                Array.Copy(delayFactors, extended, delayFactors.Length);
            }

            foreach (var settings in _settings)
                settings.DelayFactors = extended;
            return this;
        }

        public MessageErrorPolicySetup ErrorQueue(IMessageInboxWriter inbox)
        {
            foreach (var settings in _settings)
                settings.ErrorQueueWriter = inbox;
            return this;
        }

        public MessageErrorPolicySetup Drop()
        {
            return ErrorQueue(null);
        }
    }
}
