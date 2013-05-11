using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.InFile
{
    public interface IPagedFile : IDisposable
    {
        int GetSize();
        void SetSize(int pages);
        byte[] GetPage(int page);
        void SetPage(int page, byte[] data);
    }

    public class PagedFile : IPagedFile
    {
        private const int PageSize = 4096;
        private Stream _stream;
        private int _size = 0;

        public PagedFile(Stream stream)
        {
            _stream = stream;
            _size = (int)(stream.Length / PageSize);
        }
        public int GetSize()
        {
            return _size;
        }

        public void SetSize(int pages)
        {
            _size = pages;
            _stream.SetLength(PageSize * pages);
        }

        public byte[] GetPage(int page)
        {
            if (page > _size)
                throw new ArgumentOutOfRangeException();
            var buffer = new byte[PageSize];
            _stream.Seek(page * PageSize, SeekOrigin.Begin);
            _stream.Read(buffer, 0, PageSize);
            return buffer;
        }

        public void SetPage(int page, byte[] data)
        {
            if (page > _size)
                throw new ArgumentOutOfRangeException();
            if (data.Length > PageSize)
                throw new ArgumentOutOfRangeException();

            var buffer = new byte[PageSize];
            Array.Copy(data, buffer, Math.Min(PageSize, data.Length));
            _stream.Seek(page * PageSize, SeekOrigin.Begin);
            _stream.Write(buffer, 0, PageSize);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
