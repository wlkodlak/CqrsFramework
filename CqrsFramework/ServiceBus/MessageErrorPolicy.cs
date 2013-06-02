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
        public MessageErrorPolicySetup Default()
        {
            return null;
        }

        public MessageErrorPolicySetup For<T>() where T : Exception
        {
            return null;
        }

        public void Register(MessageErrorPolicySetup setup)
        {
        }

        public void HandleException(IMessageInboxReader inbox, Message message, Exception exception)
        {
            throw new NotImplementedException();
        }
    }

    public class MessageErrorPolicySetup
    {
        public MessageErrorPolicySetup And<T>() where T : Exception
        {
            return null;
        }

        public MessageErrorPolicySetup Retry(int count, params int[] delayFactors)
        {
            return null;
        }

        public MessageErrorPolicySetup ErrorQueue(IMessageInboxWriter inbox)
        {
            return null;
        }
    }
}
