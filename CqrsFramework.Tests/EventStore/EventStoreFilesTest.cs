using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.EventStore;
using CqrsFramework.IndexTable;
using CqrsFramework.Infrastructure;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreFilesTest : EventStoreTestBase
    {
        private class Builder : IEventStoreTestBuilder
        {
            SharedMemoryStreamBuffer _dataFileBuffer, _indexFileBuffer;
            FileEventStoreDataFile _dataFile;
            IIdxContainer _indexContainer;
            FileEventStoreIndexCore _indexCore;

            public Builder()
            {
                _dataFileBuffer = new SharedMemoryStreamBuffer(0);
                _indexFileBuffer = new SharedMemoryStreamBuffer(0);
                _dataFile = new FileEventStoreDataFile(new SharedMemoryStream(_dataFileBuffer));
                _indexContainer = IdxContainer.OpenStream(new SharedMemoryStream(_indexFileBuffer));
                _indexCore = new FileEventStoreIndexCore(new IdxTree(_indexContainer, 0), new IdxTree(_indexContainer, 1));
            }

            public void Build()
            {
            }

            public IEventStore GetFull()
            {
                return new FileEventStore(new SharedMemoryStream(_dataFileBuffer), new SharedMemoryStream(_indexFileBuffer));
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
            }


            public IEventStoreReader GetReader()
            {
                return new FileEventStoreReader(new SharedMemoryStream(_dataFileBuffer));
            }
        }

        protected override IEventStoreTestBuilder CreateBuilder()
        {
            return new Builder();
        }
    }
}
