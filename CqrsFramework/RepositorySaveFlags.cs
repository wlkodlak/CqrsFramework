using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework
{
    public class RepositorySaveFlags
    {
        private int _version;
        private int _snapshotLimit;

        private RepositorySaveFlags(int version, int snapshotLimit)
        {
            _version = version;
            _snapshotLimit = snapshotLimit;
        }

        public static RepositorySaveFlags Create
        {
            get { return new RepositorySaveFlags(0, -1); }
        }

        public static RepositorySaveFlags Append
        {
            get { return new RepositorySaveFlags(-1, -1); }
        }

        public RepositorySaveFlags ToVersion(int version)
        {
            return new RepositorySaveFlags(version, _snapshotLimit);
        }

        public RepositorySaveFlags WithoutSnapshot
        {
            get { return new RepositorySaveFlags(_version, 0); }
        }

        public RepositorySaveFlags WithSnapshot
        {
            get { return new RepositorySaveFlags(_version, 1); }
        }

        public RepositorySaveFlags WithSnapshotFor(int numberOfEvents)
        {
            return new RepositorySaveFlags(_version, numberOfEvents);
        }

        
        
        public bool IsModeCreateNew()
        {
            return _version == 0;
        }

        public bool IsModeAppend()
        {
            return _version != 0;
        }

        public bool HasExpectedVersion()
        {
            return _version > 0;
        }

        public bool HasSnapshotLimit()
        {
            return _snapshotLimit >= 0;
        }

        public int ExpectedVersion()
        {
            return _version;
        }

        public int SnapshotLimit()
        {
            return _snapshotLimit;
        }
    }
}
