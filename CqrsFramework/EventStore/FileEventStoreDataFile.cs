using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    public class FileEventStoreDataFile : IDisposable
    {
        private Stream _stream;
        private long _appendPosition;
        private long _lastVerifiedPosition = 0;

        public FileEventStoreDataFile(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public void SetAppendPosition(long position)
        {
            if (_stream.Length < position)
                throw new ArgumentOutOfRangeException("Trying to set append position after stream");
            else if (position < _lastVerifiedPosition)
                throw new ArgumentOutOfRangeException("Trying to set append position before last verified position");
            else if (position == _lastVerifiedPosition)
                _appendPosition = _lastVerifiedPosition;
            else if ((position & 3) != 0)
                throw new ArgumentOutOfRangeException("Position must be padded to 4 bytes");
            else
            {
                _stream.Seek(position - 4, SeekOrigin.Begin);
                var bytes = new byte[4];
                _stream.Read(bytes, 0, 4);
                if (VerifyEnd(bytes))
                    _appendPosition = position;
                else
                    throw new ArgumentOutOfRangeException("Previous entry ending was not valid");
            }

        }

        private bool VerifyEnd(byte[] bytes)
        {
            for (int i = 3; i >= 0; i--)
            {
                if (bytes[i] == 0xC7)
                    return true;
                else if (bytes[i] != 0x4E)
                    return false;
            }
            return false;
        }

        public FileEventStoreEntry ReadEntry(long position)
        {
            try
            {
                if (_stream.Length <= position)
                    return null;
                _stream.Seek(position, SeekOrigin.Begin);
                using (var reader = new BinaryReader(_stream, Encoding.ASCII, true))
                {
                    var flags = (int)reader.ReadByte();
                    if ((flags & 0x7C) != 0x5C)
                        throw new InvalidDataException("PANIC! Position does not point to valid entry");
                    var keyLength = reader.ReadByte();
                    var dataLength = reader.ReadUInt16();
                    var version = reader.ReadInt32();
                    var clock = reader.ReadInt64();
                    var name = reader.ReadBytes(keyLength);
                    var data = reader.ReadBytes(dataLength);
                    var endMarkLength = EndMarkLength(keyLength, dataLength);
                    var endMark = reader.ReadBytes(1 + endMarkLength);

                    var entry = new FileEventStoreEntry();
                    entry.Position = position;
                    entry.Published = (flags & 0x80) != 0;
                    entry.IsEvent = (flags & 0x03) == 0;
                    entry.IsSnapshot = (flags & 0x03) == 1;
                    entry.Key = Encoding.ASCII.GetString(name);
                    entry.Version = version;
                    entry.Clock = clock;
                    entry.Data = data;
                    entry.NextPosition = position + 9 + 8 + keyLength + dataLength + endMarkLength;

                    if (endMark[0] != 0xC7)
                        throw new InvalidDataException("PANIC! Event store data file entry not valid");

                    if (_lastVerifiedPosition < entry.NextPosition)
                    {
                        _lastVerifiedPosition = entry.NextPosition;
                        if (_lastVerifiedPosition >= _stream.Length)
                            _appendPosition = _lastVerifiedPosition;
                    }

                    return entry;
                }
            }
            catch (EndOfStreamException)
            {
                _appendPosition = _lastVerifiedPosition;
                return null;
            }
        }

        private static int EndMarkLength(byte keyLength, ushort dataLength)
        {
            int baseLength = (keyLength + dataLength + 1) % 4;
            return (baseLength == 0) ? 0 : 4 - baseLength;
        }

        public void AppendEntry(FileEventStoreEntry entry)
        {
            GotoAppendPosition();
            using (var writer = new BinaryWriter(_stream, Encoding.ASCII, true))
            {
                byte flags = 0x5C;
                if (entry.IsSnapshot)
                    flags |= (byte)1;
                if (entry.Published)
                    flags |= (byte)0x80;
                byte[] nameBytes = Encoding.ASCII.GetBytes(entry.Key);
                byte nameLength = (byte)nameBytes.Length;
                ushort dataLength = (ushort)entry.Data.Length;
                var endMarkLength = EndMarkLength(nameLength, dataLength);

                entry.Position = _appendPosition;
                entry.NextPosition = _appendPosition + 9 + 8 + nameLength + dataLength + endMarkLength;
                entry.Clock = _appendPosition;

                writer.Write(flags);
                writer.Write(nameLength);
                writer.Write(dataLength);
                writer.Write(entry.Version);
                writer.Write(entry.Clock);
                writer.Write(nameBytes, 0, nameLength);
                writer.Write(entry.Data, 0, dataLength);
                writer.Write((byte)0xC7);
                for (int i = 0; i < endMarkLength; i++)
                    writer.Write((byte)0x4E);

                _appendPosition = entry.NextPosition;
            }
        }

        private void GotoAppendPosition()
        {
            if (_stream.Length == 0)
                _stream.Seek(0, SeekOrigin.Begin);
            else if (_appendPosition > 0)
                _stream.Seek(_appendPosition, SeekOrigin.Begin);
            else
            {
                var existingEntry = ReadEntry(_lastVerifiedPosition);
                while (existingEntry != null)
                    existingEntry = ReadEntry(existingEntry.NextPosition);
                _stream.Seek(_appendPosition, SeekOrigin.Begin);
            }
        }

        public void MarkAsPublished(long position)
        {
            _stream.Seek(position, SeekOrigin.Begin);
            var buffer = new byte[1];
            _stream.Read(buffer, 0, 1);
            if ((buffer[0] & 0x80) == 0)
            {
                buffer[0] |= 0x80;
                _stream.Seek(position, SeekOrigin.Begin);
                _stream.Write(buffer, 0, 1);
            }
        }
    }
}
