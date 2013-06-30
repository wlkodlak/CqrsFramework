using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public class BaseTypeMapping<T>
    {
        private class Registration
        {
            public Type RegisteredType;
            public HashSet<Type> KnownTypes;
            public T Target;
        }

        private Dictionary<Type, Registration> _registrations;
        private Dictionary<Type, List<T>> _search;

        public BaseTypeMapping()
        {
            _registrations = new Dictionary<Type, Registration>();
            _search = new Dictionary<Type, List<T>>();
        }

        public void Add(Type type, T target)
        {
            var reg = new Registration();
            reg.RegisteredType = type;
            reg.Target = target;
            reg.KnownTypes = new HashSet<Type>();
            _registrations.Add(type, reg);

            bool foundInKnownTypes = false;
            foreach (var knownType in _search)
            {
                if (type.IsAssignableFrom(knownType.Key))
                {
                    knownType.Value.Add(target);
                    reg.KnownTypes.Add(knownType.Key);
                    if (type == knownType.Key)
                        foundInKnownTypes = true;
                }
            }
            if (!foundInKnownTypes)
                CreateSearchEntry(type);
        }

        public T Get(Type type)
        {
            return GetAll(type).SingleOrDefault();
        }

        public IEnumerable<T> GetAll(Type type)
        {
            List<T> knownTypes;
            if (_search.TryGetValue(type, out knownTypes))
                return knownTypes;

            knownTypes = CreateSearchEntry(type);
            return knownTypes;
        }

        private List<T> CreateSearchEntry(Type type)
        {
            var knownTypes = new List<T>();
            foreach (var registration in _registrations.Values)
            {
                if (registration.RegisteredType.IsAssignableFrom(type))
                {
                    registration.KnownTypes.Add(type);
                    knownTypes.Add(registration.Target);
                }
            }
            _search.Add(type, knownTypes);
            return knownTypes;
        }
    }
}
