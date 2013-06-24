using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Serialization
{
    public class MessageTypeResolver : IMessageTypeResolver
    {
        private struct TypeInfo
        {
            public Type Type;
            public string[] Tags;
        }
        private Dictionary<string, TypeInfo> _nameToType;
        private Dictionary<Type, string> _typeToName;
        private Dictionary<string, List<string>> _namesByTag;

        public MessageTypeResolver()
        {
            _nameToType = new Dictionary<string, TypeInfo>(StringComparer.Ordinal);
            _typeToName = new Dictionary<Type, string>();
            _namesByTag = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        }

        public void RegisterType(Type type, string name, params string[] tags)
        {
            var allTags = new List<string>(tags);
            allTags.Add(name);
            allTags.Sort();
            _nameToType[name] = new TypeInfo { Type = type, Tags = allTags.ToArray() };
            _typeToName[type] = name;
            foreach (var tag in allTags)
            {
                List<string> names;
                if (_namesByTag.TryGetValue(tag, out names))
                    names.Add(name);
                else
                {
                    names = new List<string>();
                    names.Add(name);
                    _namesByTag[tag] = names;
                }
            }
        }

        public Type GetType(string name)
        {
            TypeInfo info;
            if (_nameToType.TryGetValue(name, out info))
                return info.Type;
            else
                return null;
        }

        public string GetName(Type type)
        {
            string name;
            _typeToName.TryGetValue(type, out name);
            return name;
        }

        public string[] GetTags(string name)
        {
            TypeInfo info;
            if (_nameToType.TryGetValue(name, out info))
                return info.Tags;
            else
                return new string[0];
        }

        public string[] GetTypes(string tag)
        {
            List<string> names;
            if (_namesByTag.TryGetValue(tag, out names))
                return names.ToArray();
            else
                return new string[0];
        }
    }
}
