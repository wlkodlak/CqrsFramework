using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public interface IRebuildableProjection
    {
        void BeginUpdate();
        void EndUpdate();
        void Reset();
        bool NeedsRebuild();
        long GetClockToHandle();
    }

    public interface IProjectionDispatcher : IRebuildableProjection, IMessageDispatcher
    {
    }

    public class ProjectionDispatcher : IProjectionDispatcher
    {
        private IRebuildableProjection _projection;
        private MessageDispatcher _dispatcher;

        public ProjectionDispatcher(IRebuildableProjection projection)
        {
            _projection = projection;
            _dispatcher = new MessageDispatcher();
            _dispatcher.ThrowOnUnknownHandler = false;
        }

        public void BeginUpdate()
        {
            _projection.BeginUpdate();
        }

        public void EndUpdate()
        {
            _projection.EndUpdate();
        }

        public void Reset()
        {
            _projection.Reset();
        }

        public bool NeedsRebuild()
        {
            return _projection.NeedsRebuild();
        }

        public long GetClockToHandle()
        {
            return _projection.GetClockToHandle();
        }

        public void Dispatch(Message message)
        {
            _dispatcher.Dispatch(message);
        }

        public void Register<T>(Action<T> handler)
        {
            if (handler.Target != _projection)
                throw new ArgumentOutOfRangeException("Handler must be in the projection");
            _dispatcher.Register<T>(handler);
        }

        public void Register<T>(Action<T, MessageHeaders> handler)
        {
            if (handler.Target != _projection)
                throw new ArgumentOutOfRangeException("Handler must be in the projection");
            _dispatcher.Register<T>(handler);
        }

        public void AutoRegister()
        {
            _dispatcher.AutoRegister(_projection);
        }
    }
}
