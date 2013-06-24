using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore
{
    public class FileEventStoreReader : IEventStoreReader
    {
        private FileEventStoreDataFile _dataFile;

        public FileEventStoreReader(Stream dataFile)
        {
            _dataFile = new FileEventStoreDataFile(dataFile);
        }

        public IEnumerable<EventStoreEvent> GetSince(long clock, int maxCount)
        {
            var entry = _dataFile.ReadEntry(clock);
            if (entry == null)
                yield break;
            while (entry != null)
            {
                if (entry.IsEvent)
                {
                    yield return new EventStoreEvent
                    {
                        Published = entry.Published,
                        Key = entry.Key,
                        Version = entry.Version,
                        Data = entry.Data,
                        Clock = entry.Clock
                    };
                }
                entry = _dataFile.ReadEntry(entry.NextPosition);
            }
        }

        public void Dispose()
        {
            _dataFile.Dispose();
        }
    }
}
