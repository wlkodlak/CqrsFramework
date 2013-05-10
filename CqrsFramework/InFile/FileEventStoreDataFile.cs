using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.InFile
{
    public class FileDataFile : IDisposable
    {
        private Stream _stream;
        private long _appendPosition;

        public FileDataFile(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public DataFileEntry ReadEntry(long position)
        {
            try
            {
                if (_stream.Length < position)
                    return null;
                if (_stream.Length == position)
                {
                    _appendPosition = position;
                    return null;
                }
                _stream.Seek(position, SeekOrigin.Begin);
                using (var reader = new BinaryReader(_stream, Encoding.ASCII, true))
                {
                    var flags = (int)reader.ReadByte();
                    var keyLength = reader.ReadByte();
                    var dataLength = reader.ReadUInt16();
                    var version = reader.ReadInt32();
                    var clock = reader.ReadInt64();
                    var name = reader.ReadBytes(keyLength);
                    var data = reader.ReadBytes(dataLength);
                    var endMarkLength = EndMarkLength(keyLength, dataLength);
                    var endMark = reader.ReadBytes(1 + endMarkLength);

                    var entry = new DataFileEntry();
                    entry.Position = position;
                    entry.Published = (flags & 0x80) != 0;
                    entry.IsEvent = (flags & 0x0F) == 0;
                    entry.IsSnapshot = (flags & 0x0F) == 1;
                    entry.Key = Encoding.ASCII.GetString(name);
                    entry.Version = version;
                    entry.Clock = clock;
                    entry.Data = data;
                    entry.NextPosition = position + 9 + 8 + keyLength + dataLength + endMarkLength;

                    if (endMark[0] != 0xC7)
                        throw new InvalidDataException("PANIC! Event store data file entry not valid");

                    return entry;
                }
            }
            catch (EndOfStreamException)
            {
                _appendPosition = position;
                return null;
            }
        }

        private static int EndMarkLength(byte keyLength, ushort dataLength)
        {
            int baseLength = (keyLength + dataLength + 1) % 4;
            return (baseLength == 0) ? 0 : 4 - baseLength;
        }

        public void AppendEntry(DataFileEntry entry)
        {
            GotoAppendPosition();
            using (var writer = new BinaryWriter(_stream, Encoding.ASCII, true))
            {
                byte flags = 0;
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
                var existingEntry = ReadEntry(0);
                while (existingEntry != null)
                    existingEntry = ReadEntry(existingEntry.NextPosition);
                if (_appendPosition == 0)
                    throw new InvalidDataException("PANIC! Cannot determine append position for event store data file.");
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
