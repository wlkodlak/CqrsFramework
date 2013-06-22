using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.Infrastructure
{
    public class MemoryStreamProvider : IStreamProvider
    {
        private Dictionary<string, SharedMemoryStreamBuffer> _data = new Dictionary<string, SharedMemoryStreamBuffer>();

        public byte[] GetContents(string name)
        {
            SharedMemoryStreamBuffer contents;
            if (_data.TryGetValue(name, out contents))
                return contents.GetContents();
            else
                return null;
        }

        public void SetContents(string name, byte[] buffer)
        {
            SharedMemoryStreamBuffer contents;
            if (buffer == null)
                _data.Remove(name);
            else
            {
                if (!_data.TryGetValue(name, out contents))
                    _data[name] = contents = new SharedMemoryStreamBuffer(0);
                contents.SetContents(buffer);
            }
        }

        public Stream Open(string name, FileMode fileMode)
        {
            SharedMemoryStreamBuffer contents;
            bool found = _data.TryGetValue(name, out contents);
            if (found && fileMode == FileMode.CreateNew)
                throw new IOException();
            if (!found && fileMode == FileMode.Open)
                throw new IOException();
            if (!found)
                _data[name] = contents = new SharedMemoryStreamBuffer(1024);
            return new SharedMemoryStream(contents);
        }

        public IEnumerable<string> GetStreams()
        {
            return _data.Keys.ToList();
        }

        public void Delete(string name)
        {
            _data.Remove(name);
        }
    }
}
