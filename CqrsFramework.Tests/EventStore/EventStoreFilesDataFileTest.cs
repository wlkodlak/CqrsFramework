using System;
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.InFile;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreFilesDataFileTest
    {
        [TestMethod]
        public void ReadEntryFromEmptyFile()
        {
            using (var file = new FileDataFile(new MemoryStream()))
            {
                Assert.IsNull(file.ReadEntry(0));
            }
        }

        [TestMethod]
        public void ReadFirstEntryAtGivenPosition()
        {
            var buffer = new MemoryStream();
            using (var writer = new BinaryWriter(buffer, Encoding.ASCII, true))
            {
                WriteEntry(writer, false, false, "agg-1", 1, 2, "<agg><id>1</id></agg>");
                WriteEntry(writer, true, true, "agg-2", 1, 8, "Help me! I'm in trouble.");
            }
            buffer.Seek(0, SeekOrigin.Begin);
            using (var file = new FileDataFile(buffer))
            {
                var entry = file.ReadEntry(44);
                Assert.AreEqual(44, entry.Position);
                Assert.IsTrue(entry.Published);
                Assert.IsTrue(entry.IsSnapshot);
                Assert.AreEqual(1, entry.Version);
                Assert.AreEqual(8, entry.Clock);
                Assert.AreEqual("agg-2", entry.Key);
                Assert.AreEqual("Help me! I'm in trouble.", Encoding.ASCII.GetString(entry.Data));
                Assert.AreEqual(92, entry.NextPosition);
            }
        }

        private long WriteEntry(BinaryWriter writer, bool published, bool snapshot, string name, int version, long clock, string dataAsString)
        {
            var data = Encoding.ASCII.GetBytes(dataAsString);
            byte flags = 0;
            if (published)
                flags |= (byte)0x80;
            if (snapshot)
                flags |= (byte)1;
            byte nameLength = (byte)name.Length;
            ushort dataLength = (ushort)data.Length;
            byte[] nameBytes = Encoding.ASCII.GetBytes(name);
            writer.Write(flags);
            writer.Write(nameLength);
            writer.Write(dataLength);
            writer.Write(version);
            writer.Write(clock);
            writer.Write(nameBytes);
            writer.Write(data);
            writer.Write((byte)0xC7);
            var poziceVeSlove = (nameLength + dataLength + 1) % 4;
            for (int i = poziceVeSlove; i < 4 && i != 0; i++)
                writer.Write((byte)0x4E);
            return writer.BaseStream.Position;
        }

        [TestMethod]
        public void AppendEntry()
        {
            var buffer = new MemoryStream();
            using (var writer = new BinaryWriter(buffer, Encoding.ASCII, true))
            {
                WriteEntry(writer, false, false, "agg-1", 1, 3, "<agg><id>1</id></agg>");
                WriteEntry(writer, true, true, "agg-2", 1, 9, "Help me! I'm in trouble.");
                                                            
            }
            buffer.Seek(0, SeekOrigin.Begin);
            using (var file = new FileDataFile(buffer))
            {
                var entry = new DataFileEntry();
                entry.Data = Encoding.ASCII.GetBytes("Trying a new stuff");
                entry.Key = "agg-34";
                entry.Version = 493;
                entry.Clock = 110;
                entry.IsEvent = true;
                entry.Published = false;
                file.AppendEntry(entry);
            }

            var expectedStream = new MemoryStream();
            WriteEntry(new BinaryWriter(expectedStream, Encoding.ASCII, true), false, false, "agg-34", 493, 110, "Trying a new stuff");
            var expectedBytes = expectedStream.ToArray();

            buffer = new MemoryStream(buffer.ToArray());
            buffer.Seek(92, SeekOrigin.Begin);
            Assert.AreEqual(136, buffer.Length);
            var actualBytes = new byte[44];
            buffer.Read(actualBytes, 0, 44);
            CollectionAssert.AreEqual(expectedBytes, actualBytes);
        }

        [TestMethod]
        public void MarkAsPublished()
        {
            var buffer = new MemoryStream();
            using (var writer = new BinaryWriter(buffer, Encoding.ASCII, true))
            {
                WriteEntry(writer, false, false, "agg-1", 1, 20, "<agg><id>1</id></agg>");
                WriteEntry(writer, true, true, "agg-2", 1, 30, "Help me! I'm in trouble.");
                                                            
            }
            buffer.Seek(0, SeekOrigin.Begin);
            using (var file = new FileDataFile(buffer))
            {
                file.MarkAsPublished(0);
            }

            var expectedStream = new MemoryStream();
            WriteEntry(new BinaryWriter(expectedStream, Encoding.ASCII, true), true, false, "agg-1", 1, 20, "<agg><id>1</id></agg>");
            var expectedBytes = expectedStream.ToArray();

            buffer = new MemoryStream(buffer.ToArray());
            buffer.Seek(0, SeekOrigin.Begin);
            var actualBytes = new byte[44];
            buffer.Read(actualBytes, 0, 44);
            CollectionAssert.AreEqual(expectedBytes, actualBytes);
        }
    }
}
