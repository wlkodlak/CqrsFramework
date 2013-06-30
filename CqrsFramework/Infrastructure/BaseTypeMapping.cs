using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public class BaseTypeMapping<T>
    {
        private Dictionary<Type, T> _registrations = new Dictionary<Type, T>();

        public void Add(Type type, T target)
        {
            _registrations.Add(type, target);
        }

        public T Get(Type type)
        {
            return GetAll(type).SingleOrDefault();
        }

        public IEnumerable<T> GetAll(Type type)
        {
            var list = new List<T>();
            foreach (var pair in _registrations)
            {
                if (pair.Key.IsAssignableFrom(type))
                    list.Add(pair.Value);
            }
            return list;
        }
    }
}
