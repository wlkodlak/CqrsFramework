﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Infrastructure;
using System.IO;
using CqrsFramework.Serialization;

namespace CqrsFramework.KeyValueStore
{
    public class FilesystemKeyValueStore : IKeyValueStore
    {
        private IStreamProvider _streams;

        public FilesystemKeyValueStore(IStreamProvider streams)
        {
            _streams = streams;
        }

        public KeyValueDocument Get(string key)
        {
            try
            {
                using (var stream = _streams.Open(key, FileMode.Open))
                {
                    return ReadExisting(key, stream);
                }
            }
            catch
            {
                return null;
            }
        }

        private int ReadWhole(Stream stream, byte[] buffer, int count)
        {
            int offset = 0;
            int read;
            while ((read = stream.Read(buffer, offset, count)) > 0)
            {
                offset += read;
                count -= read;
            }
            return offset;
        }

        public int Set(string key, int expectedVersion, byte[] data)
        {
            using (var stream = _streams.Open(key, FileMode.OpenOrCreate))
            {
                var existing = ReadExisting(key, stream);
                var version = existing == null ? 0 : existing.Version;
                VerifyVersion(key, expectedVersion, version);
                stream.Seek(0, SeekOrigin.Begin);
                Write(stream, key, version + 1, data);
                return version + 1;
            }
        }

        private void VerifyVersion(string key, int expected, int actual)
        {
            if (expected != -1 && expected != actual)
                throw KeyValueStoreException.BadVersion(key, expected, actual);
        }

        private void Write(Stream stream, string key, int version, byte[] data)
        {
            var versionBytes = ByteArrayUtils.HexInt(version);
            stream.Write(versionBytes, 0, versionBytes.Length);
            stream.Write(new byte[2] { 0x0d, 0x0a }, 0, 2);
            stream.Write(data, 0, data.Length);
        }

        private KeyValueDocument ReadExisting(string key, Stream stream)
        {
            if (stream.Length < 10)
                return null;
            var contentLength = (int)(stream.Length - 10);
            var versionBytes = new byte[10];
            var contentBytes = new byte[contentLength];
            ReadWhole(stream, versionBytes, 10);
            ReadWhole(stream, contentBytes, contentLength);
            var version = ByteArrayUtils.HexInt(versionBytes);
            return new KeyValueDocument(key, version, contentBytes);
        }

        public IEnumerable<string> Enumerate()
        {
            return _streams.GetStreams();
        }

        public void Flush()
        {
        }

        public void Purge()
        {
            _streams.GetStreams().ToList().ForEach(_streams.Delete);
        }

        public void Dispose()
        {
        }
    }
}
