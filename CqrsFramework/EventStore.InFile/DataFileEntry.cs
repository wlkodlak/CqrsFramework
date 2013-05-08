using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.EventStore.InFile
{
    public class DataFileEntry
    {
        public long Position;
        public long NextPosition;
        public bool Published;
        public bool IsEvent;
        public bool IsSnapshot;
        public string Key;
        public int Version;
        public byte[] Data;
        public long Clock;
    }
}
