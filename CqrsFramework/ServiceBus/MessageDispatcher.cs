using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;

namespace CqrsFramework.ServiceBus
{
    public interface IMessageDispatcherRegistrator
    {
        void RegisterToDispatcher(MessageDispatcher dispatcher);
    }
    public class MessageDispatcher : IMessageDispatcher
    {
        private Dictionary<Type, Registration> _registrations;

        private class Registration
        {
            public Type Type;
            public Action<object, MessageHeaders> Handler;
            public Registration(Type type, Action<object, MessageHeaders> handler)
            {
                this.Type = type;
                this.Handler = handler;
            }
        }

        public MessageDispatcher()
        {
            _registrations = new Dictionary<Type, Registration>();
        }

        public MessageDispatcher(object container)
            : this()
        {
            AutoRegister(container);
        }

        public MessageDispatcher(IMessageDispatcherRegistrator registrator)
            : this()
        {
            UseRegistrator(registrator);
        }

        public void Register<T>(Action<T> handler)
        {
            var paramObject = Expression.Parameter(typeof(object));
            var paramHeaders = Expression.Parameter(typeof(MessageHeaders));
            var lambda = Expression.Lambda<Action<object, MessageHeaders>>(
                Expression.Invoke(Expression.Constant(handler), Expression.Convert(paramObject, typeof(T))),
                paramObject, paramHeaders).Compile();
            RegisterInternal(new Registration(typeof(T), lambda));
        }

        public void Register<T>(Action<T, MessageHeaders> handler)
        {
            var paramObject = Expression.Parameter(typeof(object));
            var paramHeaders = Expression.Parameter(typeof(MessageHeaders));
            var lambda = Expression.Lambda<Action<object, MessageHeaders>>(
                Expression.Invoke(Expression.Constant(handler), Expression.Convert(paramObject, typeof(T)), paramHeaders),
                paramObject, paramHeaders).Compile();
            RegisterInternal(new Registration(typeof(T), lambda));
        }

        public void UseRegistrator(IMessageDispatcherRegistrator registrator)
        {
            registrator.RegisterToDispatcher(this);
        }

        public void AutoRegister(object container)
        {
            var methods = container.GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public);
            foreach (var method in methods)
            {
                var parameters = method.GetParameters();
                if (parameters.Length == 0 || parameters.Length > 2)
                    continue;
                var parameterType = parameters[0].ParameterType;
                if (!AutoRegisterName(method.Name, parameterType))
                    continue;
                if (parameters.Length == 2 && parameters[1].ParameterType != typeof(MessageHeaders))
                    continue;

                var paramObject = Expression.Parameter(typeof(object));
                var paramHeaders = Expression.Parameter(typeof(MessageHeaders));
                var convertedObject = Expression.Convert(paramObject, parameterType);
                var body = (parameters.Length == 1)
                    ? Expression.Call(Expression.Constant(container), method, convertedObject)
                    : Expression.Call(Expression.Constant(container), method, convertedObject, paramHeaders);
                var lambda = Expression.Lambda<Action<object, MessageHeaders>>(body, paramObject, paramHeaders).Compile();
                RegisterInternal(new Registration(parameterType, lambda));
            }
        }

        private bool AutoRegisterName(string name, Type type)
        {
            if (name == "When" || name == "Handle")
                return true;
            if (name == string.Format("On{0}", type.Name))
                return true;
            return false;
        }

        private void RegisterInternal(Registration registration)
        {
            _registrations[registration.Type] = registration;
        }

        public void Dispatch(Message message)
        {
            var type = message.Payload.GetType();
            var registration = FindHandlerByType(type);
            if (registration == null)
                throw new MessageDispatcherException(string.Format("There is no handler for type {0}", type.FullName));
            registration.Handler(message.Payload, message.Headers);
        }

        private Registration FindHandlerByType(Type type)
        {
            Registration registration;
            while (type != typeof(object))
            {
                if (_registrations.TryGetValue(type, out registration))
                    return registration;
                type = type.BaseType;
            }
            return null;
        }
    }

    [Serializable]
    public class MessageDispatcherException : Exception
    {
        public MessageDispatcherException() { }
        public MessageDispatcherException(string message) : base(message) { }
        public MessageDispatcherException(string message, Exception inner) : base(message, inner) { }
        protected MessageDispatcherException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}
