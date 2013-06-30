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

        private struct SearchInfo
        {
            public List<Registration> All, Near;
        }

        private Dictionary<Type, Registration> _registrations;
        private Dictionary<Type, SearchInfo> _search;

        public BaseTypeMapping()
        {
            _registrations = new Dictionary<Type, Registration>();
            _search = new Dictionary<Type, SearchInfo>();
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
                    knownType.Value.All.Add(reg);
                    knownType.Value.Near.Clear();
                    knownType.Value.Near.Add(reg);
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
            var results = GetInternal(type);
            return results.Near.Select(r => r.Target).SingleOrDefault();
        }

        public IEnumerable<T> GetAll(Type type)
        {
            return GetInternal(type).All.Select(r => r.Target).ToList();
        }

        public IEnumerable<T> GetNearests(Type type)
        {
            return GetInternal(type).Near.Select(r => r.Target).ToList();
        }

        private SearchInfo GetInternal(Type type)
        {
            SearchInfo knownTypes;
            if (!_search.TryGetValue(type, out knownTypes))
                knownTypes = CreateSearchEntry(type);
            return knownTypes;
        }

        private SearchInfo CreateSearchEntry(Type type)
        {
            var knownTypes = new SearchInfo();
            knownTypes.All = new List<Registration>();
            knownTypes.Near = new List<Registration>();

            bool directNear = false;
            foreach (var registration in _registrations.Values)
            {
                if (registration.RegisteredType == type)
                {
                    registration.KnownTypes.Add(type);
                    knownTypes.All.Add(registration);
                    knownTypes.Near.Add(registration);
                    directNear = true;
                }
                else if (registration.RegisteredType.IsAssignableFrom(type))
                {
                    registration.KnownTypes.Add(type);
                    knownTypes.All.Add(registration);
                }
            }

            if (!directNear)
            {
                var allowedTypes = new HashSet<Type>(knownTypes.All.Select(r => r.RegisteredType));

                foreach (var registration in knownTypes.All.Where(r => r.RegisteredType != type))
                {
                    var typesToRemove = GetInternal(registration.RegisteredType).All
                        .Where(r => registration.RegisteredType != r.RegisteredType)
                        .Select(r => r.RegisteredType).ToList();
                    foreach (var t in typesToRemove)
                        allowedTypes.Remove(t);
                }

                knownTypes.Near.AddRange(knownTypes.All.Where(r => allowedTypes.Contains(r.RegisteredType)));
            }

            _search.Add(type, knownTypes);
            return knownTypes;
        }
    }
}
