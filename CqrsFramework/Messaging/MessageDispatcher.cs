﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.Messaging
{
    public interface IMessageDispatcherRegistrator
    {
        void RegisterToDispatcher(MessageDispatcher dispatcher);
    }
    public class MessageDispatcher : IMessageDispatcher
    {
        private BaseTypeMapping<Action<object, MessageHeaders>> _registrations;

        public MessageDispatcher()
        {
            _registrations = new BaseTypeMapping<Action<object, MessageHeaders>>();
            this.ThrowOnUnknownHandler = true;
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
            _registrations.Add(typeof(T), lambda);
        }

        public void Register<T>(Action<T, MessageHeaders> handler)
        {
            var paramObject = Expression.Parameter(typeof(object));
            var paramHeaders = Expression.Parameter(typeof(MessageHeaders));
            var lambda = Expression.Lambda<Action<object, MessageHeaders>>(
                Expression.Invoke(Expression.Constant(handler), Expression.Convert(paramObject, typeof(T)), paramHeaders),
                paramObject, paramHeaders).Compile();
            _registrations.Add(typeof(T), lambda);
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
                _registrations.Add(parameterType, lambda);
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

        public void Dispatch(Message message)
        {
            var type = message.Payload.GetType();
            var handler = _registrations.Get(type);
            if (handler != null)
                handler(message.Payload, message.Headers);
            else if (ThrowOnUnknownHandler)
                throw new MessageDispatcherException(string.Format("There is no handler for type {0}", type.FullName));
        }

        public bool ThrowOnUnknownHandler { get; set; }
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
