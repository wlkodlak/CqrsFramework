using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Interfaces;

namespace Vydejna.Domain
{
    public interface ICqrsAggregate
    {
        void LoadHistory(object snapshot, IEvent[] history);
        Guid Id { get; }
        int Version { get; }
        object GetSnapshot();
        IEvent[] GetUncommittedEvents();
        void MarkAsCommitted();
    }
    public abstract class CqrsAggregateBase : ICqrsAggregate
    {
        private Guid _id;
        private int _version;
        private List<IEvent> _events;
        private Dictionary<Type, Action<IEvent>> _eventHandlers;

        protected CqrsAggregateBase()
        {
            _id = Guid.NewGuid();
            _version = 0;
            _events = new List<IEvent>();
            _eventHandlers = new Dictionary<Type, Action<IEvent>>();
        }

        protected virtual object BuildSnapshot()
        {
            return null;
        }

        protected virtual void LoadFromSnapshot(object snapshot)
        {
        }

        protected virtual bool SupportsSnapshots()
        {
            return false;
        }

        protected void RegisterHandler<T>(Action<T> handler) where T : IEvent
        {
            var parameter = Expression.Parameter(typeof(T));
            var finalHandler = Expression.Lambda<Action<IEvent>>(
                Expression.Invoke(
                    Expression.Constant(handler), 
                    Expression.Convert(parameter, typeof(T))),
                parameter);
            _eventHandlers[typeof(T)] = finalHandler.Compile();
        }

        protected void Publish(IEvent @event)
        {
            _events.Add(@event);
            ApplyEvent(@event);
        }

        public Guid Id
        {
            get { return _id; }
            protected set { _id = value; }
        }

        public int Version
        {
            get { return _version; }
            protected set { _version = value; }
        }


        void ICqrsAggregate.LoadHistory(object snapshot, IEvent[] history)
        {
            if (snapshot != null)
            {
                if (SupportsSnapshots())
                    LoadFromSnapshot(snapshot);
                else
                    throw new InvalidOperationException(string.Format("Aggregate {0} does not support snapshots", GetType().FullName));
            }
            foreach (var @event in history)
                ApplyEvent(@event);
        }

        object ICqrsAggregate.GetSnapshot()
        {
            return BuildSnapshot();
        }

        IEvent[] ICqrsAggregate.GetUncommittedEvents()
        {
            return _events.ToArray();
        }

        void ICqrsAggregate.MarkAsCommitted()
        {
            _events.Clear();
        }

        private void ApplyEvent(IEvent @event)
        {
            Action<IEvent> action;
            var eventType = @event.GetType();
            if (!_eventHandlers.TryGetValue(eventType, out action))
                throw new InvalidOperationException(string.Format("Event handler for {0} not defined", eventType.FullName));
            else if (action != null)
                action(@event);
        }
    }
}
