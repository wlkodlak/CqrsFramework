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
        MessageErrorAction HandleException(int retryNumber, Exception exception);
    }

    public struct MessageErrorAction
    {
        private int _action;
        private TimeSpan _delay;
        private IMessageInboxWriter _error;

        public bool IsDrop { get { return _action == 0; } }
        public bool IsRetry { get { return _action == 1; } }
        public bool IsRedirect { get { return _action == 2; } }
        public TimeSpan DelayRetry { get { return _delay; } }
        public IMessageInboxWriter ErrorQueue { get { return _error; } }

        private MessageErrorAction(int action)
        {
            _action = action;
            _delay = TimeSpan.Zero;
            _error = null;
        }

        public static MessageErrorAction Drop()
        {
            return new MessageErrorAction(0);
        }

        public static MessageErrorAction Retry(TimeSpan delay)
        {
            return new MessageErrorAction(1) { _delay = delay };
        }

        public static MessageErrorAction Redirect(IMessageInboxWriter errorQueue)
        {
            return new MessageErrorAction(2) { _error = errorQueue };
        }

        public override string ToString()
        {
            switch (_action)
            {
                case 0:
                    return "Drop";
                case 1:
                    if (DelayRetry == TimeSpan.Zero)
                        return "Retry immediately";
                    else
                    return string.Format("Retry after {0} ms", DelayRetry.TotalMilliseconds);
                case 2:
                    return "Redirect";
                default:
                    return "Invalid action";
            }
        }

        public override int GetHashCode()
        {
            return _action;
        }

        public override bool Equals(object obj)
        {
            if (obj is MessageErrorAction)
                return this == (MessageErrorAction)obj;
            else
                return false;
        }

        public static bool operator ==(MessageErrorAction a, MessageErrorAction b)
        {
            return a._action == b._action && a.DelayRetry == b.DelayRetry && a.ErrorQueue == b.ErrorQueue;
        }

        public static bool operator !=(MessageErrorAction a, MessageErrorAction b)
        {
            return !(a == b);
        }

    }

    public class MessageErrorPolicy : IMessageErrorPolicy
    {
        private ITimeProvider _time;
        private BaseTypeMapping<MessageErrorPolicySettings> _settings;

        public MessageErrorPolicy(ITimeProvider time)
        {
            _time = time;
            _settings = new BaseTypeMapping<MessageErrorPolicySettings>();
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
            var settings = _settings.Get(type);
            if (settings == null)
            {
                settings = new MessageErrorPolicySettings(type);
                _settings.Add(type, settings);
                return settings;
            }
            else if (settings.Type != type)
            {
                settings = settings.CloneFor(type);
                _settings.Add(type, settings);
                return settings;
            }
            else
                return settings;
        }

        public MessageErrorAction HandleException(int retryNumber, Exception exception)
        {
            var settings = _settings.Get(exception.GetType());
            if (settings == null)
                return MessageErrorAction.Drop();
            else if (retryNumber < settings.RetryCount)
            {
                int delay = ComputeDelay(retryNumber + 1, settings.DelayFactors);
                return MessageErrorAction.Retry(TimeSpan.FromMilliseconds(delay));
            }
            else if (settings.ErrorQueueWriter != null)
                return MessageErrorAction.Redirect(settings.ErrorQueueWriter);
            else
                return MessageErrorAction.Drop();
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
