using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;

namespace CqrsFramework.EventStore
{
    public class FileEventStoreIndexCore
    {
        private IIdxTree _headers;
        private IIdxTree _streams;

        private long _appendPosition = 0;
        private long _unpublishedPosition = 0;
        private IdxKey _appendKey = IdxKey.FromBytes(new byte[1] { 0 });
        private IdxKey _unpublishedKey = IdxKey.FromBytes(new byte[1] { 1 });
        private bool _appendExists = false;
        private bool _unpublishedExists = false;
        private Encoding _keyEncoding = Encoding.ASCII;

        public FileEventStoreIndexCore(IIdxTree headers, IIdxTree streams)
        {
            this._headers = headers;
            this._streams = streams;

            LoadHeaders();
        }

        public long UnpublishedPosition
        {
            get { return _unpublishedPosition; }
            set { _unpublishedPosition = value; }
        }
        public long AppendPosition
        {
            get { return _appendPosition; }
            set { _appendPosition = value; }
        }

        public void Flush()
        {
            if (_appendExists)
                _headers.Update(_appendKey, WriteLong(_appendPosition));
            else
                _headers.Insert(_appendKey, WriteLong(_appendPosition));
            if (_unpublishedExists)
                _headers.Update(_unpublishedKey, WriteLong(_unpublishedPosition));
            else
                _headers.Insert(_unpublishedKey, WriteLong(_unpublishedPosition));
        }

        private void LoadHeaders()
        {
            foreach (var pair in _headers.Select(_appendKey, _unpublishedKey))
            {
                if (pair.Key == _appendKey)
                {
                    _appendPosition = ReadLong(pair.Value.ToArray());
                    _appendExists = true;
                }
                else if (pair.Key == _unpublishedKey)
                {
                    _unpublishedPosition = ReadLong(pair.Value.ToArray());
                    _unpublishedExists = true;
                }
            }
        }

        private long ReadLong(byte[] bytes)
        {
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return BitConverter.ToInt64(bytes, 0);
        }

        private byte[] WriteLong(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        private IdxKey StreamKey(string streamId, bool snapshot, int version)
        {
            var stringByteCount = (short)_keyEncoding.GetByteCount(streamId);
            var bytes = new byte[2 + stringByteCount + 5];
            bytes[0] = (byte)((stringByteCount >> 8) & 0xff);
            bytes[1] = (byte)((stringByteCount) & 0xff);
            _keyEncoding.GetBytes(streamId, 0, streamId.Length, bytes, 2);
            var typeOffset = 2 + stringByteCount;
            bytes[typeOffset] = snapshot ? (byte)0 : (byte)1;
            var versionOffset = typeOffset + 1;
            bytes[versionOffset + 0] = (byte)((version >> 24) & 0xff);
            bytes[versionOffset + 1] = (byte)((version >> 16) & 0xff);
            bytes[versionOffset + 2] = (byte)((version >> 8) & 0xff);
            bytes[versionOffset + 3] = (byte)((version) & 0xff);
            return IdxKey.FromBytes(bytes);
        }

        public void AddEvent(string streamKey, int version, long position)
        {
            var key = StreamKey(streamKey, false, version);
            var value = WriteLong(position);
            _streams.Insert(key, value);
        }

        public void AddSnapshot(string streamKey, int version, long position)
        {
            var key = StreamKey(streamKey, true, version);
            var value = WriteLong(position);
            _streams.Insert(key, value);
        }

        private IEnumerable<long> FindInStreams(IdxKey minKey, IdxKey maxKey)
        {
            var pairs = _streams.Select(minKey, maxKey);
            return pairs.Select(v => ReadLong(v.Value));
        }

        public IEnumerable<long> FindEvents(string streamKey, int minVersion)
        {
            return FindInStreams(StreamKey(streamKey, false, minVersion), StreamKey(streamKey, false, int.MaxValue));
        }

        public long FindSnapshot(string streamKey)
        {
            return FindInStreams(StreamKey(streamKey, true, 0), StreamKey(streamKey, true, int.MaxValue)).LastOrDefault();
        }

        public bool StreamExists(string streamKey)
        {
            return FindInStreams(StreamKey(streamKey, false, 0), StreamKey(streamKey, false, 1)).Any();
        }
    }
}
