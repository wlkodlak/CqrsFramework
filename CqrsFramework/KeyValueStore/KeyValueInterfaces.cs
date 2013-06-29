using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.KeyValueStore
{
    public class KeyValueDocument
    {
        public KeyValueDocument(string key, int version, byte[] data)
        {
            this.Key = key;
            this.Version = version;
            this.Data = data;
        }

        public string Key { get; private set; }
        public int Version { get; private set; }
        public byte[] Data { get; private set; }

        public override int GetHashCode()
        {
            return (Key ?? "").GetHashCode() ^ (Version << 16) ^ (Data.Length << 3);
        }

        public override bool Equals(object obj)
        {
            var oth = obj as KeyValueDocument;
            if (Key != oth.Key || Version != oth.Version)
                return false;
            if (Data == null)
                return oth.Data == null;
            else if (oth.Data == null || Data.Length != oth.Data.Length)
                return false;
            else
            {
                int length = Data.Length;
                for (int i = 0; i < length; i++)
                    if (Data[i] != oth.Data[i])
                        return false;
                return true;
            }
        }

        public override string ToString()
        {
            if (Data == null)
                return string.Concat(Key, " = null");
            return string.Concat(Key, " = (", Data.Length.ToString(), "B@", Version.ToString(), ")");
        }
    }

    public interface IKeyValueStore : IDisposable
    {
        KeyValueDocument Get(string key);
        int Set(string key, int expectedVersion, byte[] data);
        IEnumerable<string> Enumerate();
        void Flush();
        void Purge();
    }

    public class KeyValueStoreException : Exception
    {
        public string Key { get; private set; }
        public int ExpectedVersion { get; private set; }
        public int ActualVersion { get; private set; }

        private KeyValueStoreException(string message, string key, int expected, int actual)
            : base(message)
        {
        }

        public static KeyValueStoreException BadVersion(string key, int expected, int actual)
        {
            return new KeyValueStoreException(
                string.Format("Bad version for {0}, expected {1}, was {2}", key, expected, actual),
                key, expected, actual);
        }
    }
}
