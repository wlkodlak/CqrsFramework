using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public interface IIdxTree
    {
        void Insert(IdxKey key, byte[] value);
        void Update(IdxKey key, byte[] value);
        void Delete(IdxKey key);
        IEnumerable<KeyValuePair<IdxKey, byte[]>> Select(IdxKey min, IdxKey max);
    }

    public class IdxTree
    {
    }
}
