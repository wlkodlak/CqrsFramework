using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.EventStore;
using CqrsFramework.IndexTable;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreFilesTest : EventStoreTestBase
    {
        private class Builder : IEventStoreTestBuilder
        {
            MemoryStream _dataFileStream, _indexFileStream;
            FileEventStoreDataFile _dataFile;
            IIdxContainer _indexContainer;
            FileEventStoreIndexCore _indexCore;

            public Builder()
            {
                _dataFileStream = new MemoryStream();
                _dataFile = new FileEventStoreDataFile(_dataFileStream);
                _indexFileStream = new MemoryStream();
                _indexContainer = IdxContainer.OpenStream(_indexFileStream);
                _indexCore = new FileEventStoreIndexCore(new IdxTree(_indexContainer, 0), new IdxTree(_indexContainer, 1));
            }

            public IEventStore Build()
            {
                var finalFileStream = new MemoryStream();
                var finalIndexStream = new MemoryStream();
                _dataFileStream.WriteTo(finalFileStream);
                finalFileStream.Seek(0, SeekOrigin.Begin);
                _indexCore.Flush();
                _indexContainer.Dispose();
                var indexBytes = _indexFileStream.ToArray();
                finalIndexStream.Write(indexBytes, 0, indexBytes.Length);
                finalIndexStream.Seek(0, SeekOrigin.Begin);
                return new FileEventStore(finalFileStream, finalIndexStream);
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                if (snapshot != null)
                {
                    var entry = CreateEntry(name, snapshot);
                    _dataFile.AppendEntry(entry);
                    _indexCore.AddSnapshot(entry.Key, entry.Version, entry.Position);
                }
                foreach (var @event in events)
                {
                    var entry = CreateEntry(name, @event);
                    _dataFile.AppendEntry(entry);
                    @event.Clock = entry.Clock;
                    _indexCore.AddEvent(entry.Key, entry.Version, entry.Position);
                }
            }

            private FileEventStoreEntry CreateEntry(string name, EventStoreSnapshot snapshot)
            {
                return new FileEventStoreEntry
                {
                    IsSnapshot = true,
                    Published = true,
                    Key = name,
                    Version = snapshot.Version,
                    Clock = 0,
                    Data = snapshot.Data
                };
            }

            private FileEventStoreEntry CreateEntry(string name, EventStoreEvent @event)
            {
                return new FileEventStoreEntry
                {
                    IsEvent = true,
                    Published = @event.Published,
                    Key = name,
                    Version = @event.Version,
                    Clock = @event.Clock,
                    Data = @event.Data
                };
            }


            public void Dispose()
            {
                _dataFile.Dispose();
                _dataFileStream.Dispose();
            }
        }

        protected override IEventStoreTestBuilder CreateBuilder()
        {
            return new Builder();
        }
    }
}
