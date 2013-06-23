using CqrsFramework.Infrastructure;
using CqrsFramework.Messaging;
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
                var action = _errorPolicy.HandleException(message.Message.Headers.RetryNumber + 1, ex);
                ExceptionAction(message, action);
                return false;
            }
        }

        private void ExceptionAction(MessageWithSource message, MessageErrorAction action)
        {
            if (action.IsDrop)
                message.Inbox.Delete(message.Message);
            else if (action.IsRedirect)
            {
                action.ErrorQueue.Put(message.Message);
                message.Inbox.Delete(message.Message);
            }
            else if (action.IsRetry)
            {
                var retried = new Message(message.Message.Payload);
                retried.Headers.CopyFrom(message.Message.Headers);
                retried.Headers.RetryNumber++;
                retried.Headers.DeliverOn = _time.Get().Add(action.DelayRetry);
                message.Inbox.Put(retried);
            }
        }
    }
}
