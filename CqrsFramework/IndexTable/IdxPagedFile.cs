using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public interface IIdxPagedFile : IDisposable
    {
        int GetSize();
        void SetSize(int pages);
        byte[] GetPage(int page);
        void SetPage(int page, byte[] data);
        int PageSize { get; }
    }

    public class IdxPagedFile : IIdxPagedFile
    {
        private int _pageSize;
        private Stream _stream;
        private int _size = 0;

        public IdxPagedFile(Stream stream, int pageSize)
        {
            _stream = stream;
            _pageSize = pageSize;
            _size = (int)(stream.Length / _pageSize);
        }
        public int GetSize()
        {
            return _size;
        }

        public int PageSize { get { return _pageSize; } }

        public void SetSize(int pages)
        {
            _size = pages;
            _stream.SetLength(_pageSize * pages);
        }

        public byte[] GetPage(int page)
        {
            if (page > _size)
                throw new ArgumentOutOfRangeException();
            var buffer = new byte[_pageSize];
            _stream.Seek(page * _pageSize, SeekOrigin.Begin);
            _stream.Read(buffer, 0, _pageSize);
            return buffer;
        }

        public void SetPage(int page, byte[] data)
        {
            if (page > _size)
                throw new ArgumentOutOfRangeException();
            if (data.Length > _pageSize)
                throw new ArgumentOutOfRangeException();

            var buffer = new byte[_pageSize];
            Array.Copy(data, buffer, Math.Min(_pageSize, data.Length));
            _stream.Seek(page * _pageSize, SeekOrigin.Begin);
            _stream.Write(buffer, 0, _pageSize);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
