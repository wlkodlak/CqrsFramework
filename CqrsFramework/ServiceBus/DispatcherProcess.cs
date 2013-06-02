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
        private IMessageDeduplicator _dup;

        public DispatcherProcessCore(CancellationToken token, IPrioritizedInboxesReceiver receiver, IMessageDispatcher dispatcher, IMessageErrorPolicy errorPolicy, ITimeProvider time, IMessageDeduplicator dup)
        {
            _token = token;
            _receiver = receiver;
            _dispatcher = dispatcher;
            _errorPolicy = errorPolicy;
            _time = time;
            _dup = dup;
        }

        public async Task ProcessSingle()
        {
            var message = await _receiver.ReceiveAsync(_token);
            
            var createdOn = message.Message.Headers.CreatedOn;
            var deliverOn = message.Message.Headers.DeliverOn;
            var validUntil = message.Message.Headers.ValidUntil;

            var timedOut = validUntil != DateTime.MinValue && _time.Get() > validUntil;
            var delayed = deliverOn != DateTime.MinValue && _time.Get() < deliverOn;
            var duplicate = _dup.IsDuplicate(message.Message);

            if (timedOut || duplicate)
                message.Inbox.Delete(message.Message);
            else if (delayed)
                _receiver.PutToDelayed(deliverOn, message);
            else if (Dispatch(message))
                message.Inbox.Delete(message.Message);
        }

        private bool Dispatch(MessageWithSource message)
        {
            try
            {
                _dispatcher.Dispatch(message.Message);
                _dup.MarkHandled(message.Message);
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
