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

    public class IdxTree : IIdxTree
    {
        private IIdxContainer _container;
        private int _tree;

        public IdxTree(IIdxContainer container, int tree)
        {
            _container = container;
            _tree = tree;
        }

        public void Insert(IdxKey key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Update(IdxKey key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Delete(IdxKey key)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<KeyValuePair<IdxKey, byte[]>> Select(IdxKey min, IdxKey max)
        {
            var result = new List<KeyValuePair<IdxKey, byte[]>>();
            var root = (IdxLeaf)_container.ReadTree(_tree);
            if (root == null)
                return result;

            for (int i = 0; i < root.CellsCount; i++)
            {
                var cell = root.GetCell(i);
                if (min <= cell.Key && cell.Key <= max)
                    result.Add(new KeyValuePair<IdxKey, byte[]>(cell.Key, cell.ValueBytes));
            }

            _container.UnlockRead(_tree);
            return result;
        }
    }
}
