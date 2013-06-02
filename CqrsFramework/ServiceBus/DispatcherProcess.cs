using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.ServiceBus
{
    public class DispatcherProcessCore
    {
        private CancellationToken _token;
        private IPrioritizedInboxesReceiver _receiver;
        private IMessageDispatcher _dispatcher;
        private IMessageErrorPolicy _errorPolicy;
        private ITimeProvider _time;

        public DispatcherProcessCore(CancellationToken token, IPrioritizedInboxesReceiver receiver, IMessageDispatcher dispatcher, IMessageErrorPolicy errorPolicy, ITimeProvider time)
        {
            _token = token;
            _receiver = receiver;
            _dispatcher = dispatcher;
            _errorPolicy = errorPolicy;
            _time = time;
        }

        public async Task ProcessSingle()
        {
            var message = await _receiver.ReceiveAsync(_token);
            
            var createdOn = message.Message.Headers.CreatedOn;
            var delay = message.Message.Headers.Delay;
            var timeout = message.Message.Headers.TimeToLive;

            var timedOut = timeout != TimeSpan.Zero && _time.Get() > createdOn.Add(timeout);
            var delayed = delay != TimeSpan.Zero && _time.Get() < createdOn.Add(delay);

            if (timedOut)
                message.Inbox.Delete(message.Message);
            else if (delayed)
                _receiver.PutToDelayed(createdOn.Add(delay), message);
            else if (Dispatch(message))
                message.Inbox.Delete(message.Message);
        }

        private bool Dispatch(MessageWithSource message)
        {
            try
            {
                _dispatcher.Dispatch(message.Message);
                return true;
            }
            catch (Exception ex)
            {
                _errorPolicy.HandleException(message.Inbox, message.Message, ex);
                return false;
            }
        }
    }
}
