using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.InMemory
{
    public class MemoryStreamProvider : IStreamProvider
    {
        private class ProviderItem
        {
            public byte[] Data;
        }

        private Dictionary<string, ProviderItem> _data = new Dictionary<string, ProviderItem>();

        public byte[] GetContents(string name)
        {
            ProviderItem contents;
            if (_data.TryGetValue(name, out contents))
                return contents.Data;
            else
                return null;
        }

        public void SetContents(string name, byte[] buffer)
        {
            if (buffer == null)
                _data.Remove(name);
            else
                _data[name] = new ProviderItem { Data = buffer.ToArray() };
        }

        public Stream Open(string name, FileMode fileMode)
        {
            ProviderItem contents;
            bool found = _data.TryGetValue(name, out contents);
            if (found && fileMode == FileMode.CreateNew)
                throw new IOException();
            if (!found && fileMode == FileMode.Open)
                throw new IOException();
            if (!found)
                _data[name] = contents = new ProviderItem();
            return new SaveOnDisposeStream(contents);
        }

        private class SaveOnDisposeStream : MemoryStream
        {
            private ProviderItem _item;

            public SaveOnDisposeStream(ProviderItem item)
            {
                _item = item;
                if (_item.Data != null)
                {
                    Write(_item.Data, 0, _item.Data.Length);
                    Seek(0, SeekOrigin.Begin);
                }
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _item.Data = ToArray();
            }
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
