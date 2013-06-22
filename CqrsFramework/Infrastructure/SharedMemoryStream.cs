using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public class SharedMemoryStreamBuffer
    {
        private byte[] _buffer;
        private int _length, _capacity;

        public SharedMemoryStreamBuffer(int capacity)
        {
            _capacity = capacity;
            _length = 0;
            _buffer = new byte[_capacity];
        }

        public int GetLength()
        {
            return _length;
        }

        public int Read(byte[] buffer, int offset, int position, int count)
        {
            count = Math.Min(count, _length - position);
            if (count <= 0)
                return 0;
            Array.Copy(_buffer, position, buffer, offset, count);
            return count;
        }

        public byte[] GetContents()
        {
            var result = new byte[_length];
            Array.Copy(_buffer, result, _length);
            return result;
        }

        public void SetContents(byte[] buffer)
        {
            _buffer = new byte[buffer.Length];
            Array.Copy(buffer, _buffer, buffer.Length);
            _capacity = _length = buffer.Length;
        }

        public void SetLength(int length)
        {
            if (length == _length)
                return;
            else if (length < _length)
            {
                _length = length;
                EnsureCapacity(length);
            }
            else
            {
                EnsureCapacity(length);
                Array.Clear(_buffer, _length, length - _length);
                _length = length;
            }
        }

        public int Write(byte[] buffer, int offset, int position, int count)
        {
            var finalLength = Math.Max(position + count, _length);
            EnsureCapacity(finalLength);
            if (_length < position)
                Array.Clear(_buffer, _length, position - _length);
            Array.Copy(buffer, offset, _buffer, position, count);
            _length = finalLength;
            return count;
        }

        private void EnsureCapacity(int capacity)
        {
            if (capacity < _length)
                capacity = _length;

            if (capacity > _capacity)
            {
                if (capacity < int.MaxValue / 2)
                    capacity = Math.Max(capacity, _capacity * 2);
            }
            else if (capacity <= 1024)
                capacity = 1024;
            else if (capacity >= _capacity / 2)
                capacity = _capacity;

            if (capacity != _capacity)
            {
                Array.Resize(ref _buffer, capacity);
                _capacity = capacity;
            }
        }
    }

    public class SharedMemoryStream : Stream
    {
        private SharedMemoryStreamBuffer _buffer;
        private int _position = 0;
        private bool _disposed = false;

        public SharedMemoryStream(SharedMemoryStreamBuffer buffer)
        {
            _buffer = buffer ?? new SharedMemoryStreamBuffer(1024);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            _disposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("SharedMemoryStream");
        }

        public override bool CanRead
        {
            get { return !_disposed; }
        }

        public override bool CanSeek
        {
            get { return !_disposed; }
        }

        public override bool CanWrite
        {
            get { return !_disposed; }
        }

        public override void Flush()
        {
            ThrowIfDisposed();
        }

        public override long Length
        {
            get { ThrowIfDisposed(); return _buffer.GetLength(); }
        }

        public override long Position
        {
            get { ThrowIfDisposed(); return _position; }
            set { ThrowIfDisposed(); _position = (int)value; }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            int read = _buffer.Read(buffer, offset, _position, count);
            _position += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            ThrowIfDisposed();
            switch (origin)
            {
                case SeekOrigin.Begin:
                    _position = (int)offset;
                    return _position;
                case SeekOrigin.Current:
                    _position += (int)offset;
                    return _position;
                case SeekOrigin.End:
                    _position = _buffer.GetLength() - (int)offset;
                    return _position;
                default:
                    throw new ArgumentOutOfRangeException("origin");
            }
        }

        public override void SetLength(long value)
        {
            ThrowIfDisposed();
            _buffer.SetLength((int)value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            ThrowIfDisposed();
            _buffer.Write(buffer, offset, _position, count);
            _position += count;
        }
    }
}
