using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.IndexTable
{
    public interface IIdxFile : IDisposable
    {
        void Insert(int tree, byte[] key, byte[] value);
        void Update(int tree, byte[] key, byte[] value);
        void Delete(int tree, byte[] key);
        IEnumerable<KeyValuePair<byte[], byte[]>> Select(int tree, byte[] minKey, byte[] maxKey);
    }
}
