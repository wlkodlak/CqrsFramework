using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace CqrsFramework.Domain
{
    public interface IEvent
    {
    }

    public interface ICommand
    {
    }

    public interface IAggregate
    {
        void LoadFromHistory(object snapshot, IEnumerable<IEvent> history);
        IEnumerable<IEvent> GetEvents();
        void Commit();
        object GetSnapshot();
    }

    public abstract class AggregateBase : IAggregate
    {
        private Dictionary<Type, Action<IEvent>> _handlers;
        private List<IEvent> _events;

        public AggregateBase()
        {
            _handlers = new Dictionary<Type, Action<IEvent>>();
            _events = new List<IEvent>();
        }

        void IAggregate.LoadFromHistory(object snapshot, IEnumerable<IEvent> history)
        {
            LoadSnapshot(snapshot);
            foreach (var @event in history)
                CallHandlerFor(@event);
        }

        object IAggregate.GetSnapshot()
        {
            return BuildSnapshot(null);
        }

        IEnumerable<IEvent> IAggregate.GetEvents()
        {
            return _events;
        }

        void IAggregate.Commit()
        {
            _events.Clear();
        }

        protected void Register<T>(Action<T> handler)
        {
            _handlers[typeof(T)] = CreateHandler<T>(handler);
        }

        private Action<IEvent> CreateHandler<T>(Delegate handler)
        {
            var param = Expression.Parameter(typeof(IEvent));
            var lambda = Expression.Lambda<Action<IEvent>>(
                Expression.Invoke(
                    Expression.Constant(handler, typeof(Action<T>)),
                    Expression.Convert(param, typeof(T))),
                param);
            return lambda.Compile();
        }

        private void CallHandlerFor(IEvent @event)
        {
            _handlers[@event.GetType()](@event);
        }

        public void Publish(IEvent @event)
        {
            _events.Add(@event);
            CallHandlerFor(@event);
        }

        protected virtual object BuildSnapshot(object snapshot)
        {
            return snapshot;
        }

        protected virtual void LoadSnapshot(object snapshot)
        {
        }
    }
}
