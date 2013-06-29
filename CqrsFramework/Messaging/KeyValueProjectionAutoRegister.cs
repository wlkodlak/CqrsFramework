using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public class KeyValueProjectionAutoRegister<TView>
    {
        private object _dispatcher;

        public KeyValueProjectionAutoRegister(object dispatcher)
        {
            _dispatcher = dispatcher;
        }

        public interface IRegistration
        {
            Type Type { get; }
        }

        private enum AutoRegisterTemplate
        {
            None, GetKey,
            FullAdd, ShortAdd,
            FullUpdate, ShortUpdate,
            FullVoid, ShortVoid
        }

        private class AutoRegisterItem : IRegistration
        {
            public Type Type { get; set; }
            public MethodInfo KeyMethod;
            public MethodInfo AddMethod;
            public MethodInfo UpdateMethod;
            public AutoRegisterTemplate AddTemplate, UpdateTemplate;
        }

        public IEnumerable<IRegistration> FindMethods()
        {
            var map = new Dictionary<Type, AutoRegisterItem>();

            foreach (var method in _dispatcher.GetType().GetMethods())
            {
                var template = DetectTemplate(method);
                if (template == AutoRegisterTemplate.None)
                    continue;
                var type = DetectType(method);
                var item = GetAutoItem(map, type);
                if (template == AutoRegisterTemplate.GetKey)
                    item.KeyMethod = method;
                else if (template == AutoRegisterTemplate.FullAdd || template == AutoRegisterTemplate.ShortAdd)
                {
                    if (item.AddTemplate < template)
                    {
                        item.AddMethod = method;
                        item.AddTemplate = template;
                    }
                }
                else
                {
                    if (item.AddTemplate < template)
                    {
                        item.AddMethod = method;
                        item.AddTemplate = template;
                    }
                    if (item.UpdateTemplate < template)
                    {
                        item.UpdateMethod = method;
                        item.UpdateTemplate = template;
                    }
                }
            }

            return map.Values.Where(VerifyMap);
        }

        private static bool VerifyMap(AutoRegisterItem item)
        {
            if (item == null || item.KeyMethod == null)
                return false;
            if (item.AddTemplate == AutoRegisterTemplate.None && item.UpdateTemplate == AutoRegisterTemplate.None)
                return false;
            return true;
        }

        private static AutoRegisterTemplate DetectTemplate(MethodInfo method)
        {
            var parameters = method.GetParameters();
            if (method.Name == "GetKey")
            {
                if (parameters.Length == 1 && method.ReturnType == typeof(string))
                    return AutoRegisterTemplate.GetKey;
                else
                    return AutoRegisterTemplate.None;
            }
            else if (IsHandlerName(method))
            {
                if (parameters.Length == 3)
                {
                    if (parameters[1].ParameterType == typeof(MessageHeaders) && parameters[2].ParameterType == typeof(TView))
                        return AutoRegisterTemplate.FullUpdate;
                    else
                        return AutoRegisterTemplate.None;
                }
                else if (parameters.Length == 2)
                {
                    if (parameters[1].ParameterType == typeof(MessageHeaders))
                        return method.ReturnType == typeof(TView) ? AutoRegisterTemplate.FullAdd : AutoRegisterTemplate.None;
                    else if (parameters[1].ParameterType == typeof(TView))
                    {
                        if (method.ReturnType == typeof(TView))
                            return AutoRegisterTemplate.ShortUpdate;
                        else if (method.ReturnType == typeof(void))
                            return AutoRegisterTemplate.ShortVoid;
                        else
                            return AutoRegisterTemplate.None;
                    }
                    else
                        return AutoRegisterTemplate.None;
                }
                else if (parameters.Length == 1)
                    return method.ReturnType == typeof(TView) ? AutoRegisterTemplate.ShortAdd : AutoRegisterTemplate.None;
                else
                    return AutoRegisterTemplate.None;
            }
            else
                return AutoRegisterTemplate.None;
        }

        private static bool IsHandlerName(MethodInfo method)
        {
            return method.Name == "When" || method.Name == "Handle" ||
                method.Name == "Add" || method.Name == "Modify" || method.Name.StartsWith("On");
        }

        private static Type DetectType(MethodInfo method)
        {
            return method.GetParameters()[0].ParameterType;
        }

        private static AutoRegisterItem GetAutoItem(Dictionary<Type, AutoRegisterItem> map, Type type)
        {
            AutoRegisterItem item;
            if (map.TryGetValue(type, out item))
                return item;
            item = new AutoRegisterItem();
            item.Type = type;
            map[type] = item;
            return item;
        }

        public Func<object, string> MakeGetKey(IRegistration registration)
        {
            var item = registration as AutoRegisterItem;
            return CreateGetKeyDelegate(_dispatcher, item.Type, item.KeyMethod);
        }

        private static Func<object, string> CreateGetKeyDelegate(object dispatcher, Type type, MethodInfo method)
        {
            if (method == null)
                return null;
            var param1 = Expression.Parameter(typeof(object));
            var lambda = Expression.Lambda<Func<object, string>>(
                Expression.Call(Expression.Constant(dispatcher), method, Expression.Convert(param1, type)),
                param1);
            return lambda.Compile();
        }

        public Func<object, MessageHeaders, TView> MakeAdd(IRegistration registration)
        {
            var item = registration as AutoRegisterItem;
            return CreateAddDelegate(_dispatcher, item.Type, item.AddMethod, item.AddTemplate);
        }

        private static Func<object, MessageHeaders, TView> CreateAddDelegate(object dispatcher, Type type, MethodInfo method, AutoRegisterTemplate template)
        {
            if (method == null)
                return null;
            var param1 = Expression.Parameter(typeof(object));
            var param2 = Expression.Parameter(typeof(MessageHeaders));
            var newView = Expression.New(typeof(TView).GetConstructor(Type.EmptyTypes));
            var varView = Expression.Variable(typeof(TView));
            var callParams = new List<Expression>();
            callParams.Add(Expression.Convert(param1, type));
            if (template == AutoRegisterTemplate.FullAdd || template == AutoRegisterTemplate.FullUpdate || template == AutoRegisterTemplate.FullVoid)
                callParams.Add(param2);
            if (template == AutoRegisterTemplate.FullUpdate || template == AutoRegisterTemplate.ShortUpdate)
                callParams.Add(newView);
            if (template == AutoRegisterTemplate.FullVoid || template == AutoRegisterTemplate.ShortVoid)
                callParams.Add(varView);
            var call = Expression.Call(Expression.Constant(dispatcher), method, callParams.ToArray());
            Expression body;
            if (template == AutoRegisterTemplate.FullVoid || template == AutoRegisterTemplate.ShortVoid)
                body = Expression.Block(typeof(TView), new[] { varView }, new Expression[] { call, varView });
            else
                body = call;
            var lambda = Expression.Lambda<Func<object, MessageHeaders, TView>>(body, param1, param2);
            return lambda.Compile();
        }

        public Func<object, MessageHeaders, TView, TView> MakeUpdate(IRegistration registration)
        {
            var item = registration as AutoRegisterItem;
            return CreateUpdateDelegate(_dispatcher, item.Type, item.UpdateMethod, item.UpdateTemplate);
        }

        private static Func<object, MessageHeaders, TView, TView> CreateUpdateDelegate(object dispatcher, Type type, MethodInfo method, AutoRegisterTemplate template)
        {
            if (method == null)
                return null;
            var param1 = Expression.Parameter(typeof(object));
            var param2 = Expression.Parameter(typeof(MessageHeaders));
            var param3 = Expression.Parameter(typeof(TView));
            var callParams = new List<Expression>();
            callParams.Add(Expression.Convert(param1, type));
            if (template == AutoRegisterTemplate.FullUpdate || template == AutoRegisterTemplate.FullVoid)
                callParams.Add(param2);
            if (template == AutoRegisterTemplate.FullUpdate || template == AutoRegisterTemplate.ShortUpdate)
                callParams.Add(param3);
            if (template == AutoRegisterTemplate.FullVoid || template == AutoRegisterTemplate.ShortVoid)
                callParams.Add(param3);
            var call = Expression.Call(Expression.Constant(dispatcher), method, callParams.ToArray());
            Expression body;
            if (template == AutoRegisterTemplate.FullVoid || template == AutoRegisterTemplate.ShortVoid)
                body = Expression.Block(typeof(TView), new Expression[] { call, param3 });
            else
                body = call;
            var lambda = Expression.Lambda<Func<object, MessageHeaders, TView, TView>>(body, param1, param2, param3);
            return lambda.Compile();
        }
    }
}
