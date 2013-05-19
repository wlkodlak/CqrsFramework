using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CqrsFramework.InFile;

namespace CqrsFramework.Tests.EventStore
{
    [TestClass]
    public class EventStoreFilesTest : EventStoreTestBase
    {
        private class Builder : IEventStoreTestBuilder
        {
            MemoryStream _dataFileStream;
            FileDataFile _dataFile;

            public Builder()
            {
                _dataFileStream = new MemoryStream();
                _dataFile = new FileDataFile(_dataFileStream);
            }

            public IEventStore Build()
            {
                var finalFileStream = new MemoryStream();
                var finalIndexStream = new MemoryStream();
                _dataFileStream.WriteTo(finalFileStream);
                finalFileStream.Seek(0, SeekOrigin.Begin);
                return new FileEventStore(finalFileStream, finalIndexStream);
            }

            public void WithStream(string name, EventStoreSnapshot snapshot, EventStoreEvent[] events)
            {
                if (snapshot != null)
                    _dataFile.AppendEntry(CreateEntry(name, snapshot));
                foreach (var @event in events)
                    _dataFile.AppendEntry(CreateEntry(name, @event));
            }

            private DataFileEntry CreateEntry(string name, EventStoreSnapshot snapshot)
            {
                return new DataFileEntry
                {
                    IsSnapshot = true,
                    Published = true,
                    Key = name,
                    Version = snapshot.Version,
                    Clock = 0,
                    Data = snapshot.Data
                };
            }

            private DataFileEntry CreateEntry(string name, EventStoreEvent @event)
            {
                return new DataFileEntry
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
