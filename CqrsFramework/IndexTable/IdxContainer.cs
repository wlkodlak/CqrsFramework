using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public interface IIdxContainer : IDisposable
    {
        IIdxNode ReadTree(int tree);
        IIdxNode WriteTree(int tree);
        void UnlockRead(int tree);
        void CommitWrite(int tree);
        void RollbackWrite(int tree);
        IdxOverflow GetOverflow(int tree, int page);
        IdxOverflow CreateOverflow(int tree, int page);
        IIdxNode GetNode(int tree, int page);
        IdxLeaf CreateLeaf(int tree);
        IdxInterior CreateInterior(int tree);
        void Delete(int tree, int page);
        void SetTreeRoot(int tree, IIdxNode root);
    }

    public class IdxContainer : IIdxContainer
    {
        private IPagedFile _file;
        private IdxHeader _header;

        public IdxContainer(IPagedFile file)
        {
            _file = file;
            if (_file.GetSize() > 0)
                _header = new IdxHeader(_file.GetPage(0));
        }

        public IIdxNode ReadTree(int tree)
        {
            if (_header == null)
                return null;
            int page = _header.GetTreeRoot(tree);
            if (page == 0)
                return null;
            return ReadNode(page);
        }

        private IIdxNode ReadNode(int page)
        {
            var bytes = _file.GetPage(page);
            IIdxNode node;
            if (DetectLeaf(bytes))
                node = new IdxLeaf(bytes);
            else
                node = new IdxInterior(bytes);
            node.PageNumber = page;
            return node;
        }

        private bool DetectLeaf(byte[] bytes)
        {
            return bytes[0] == 1;
        }

        public IIdxNode WriteTree(int tree)
        {
            throw new NotImplementedException();
        }

        public void UnlockRead(int tree)
        {
        }

        public void CommitWrite(int tree)
        {
        }

        public void RollbackWrite(int tree)
        {
        }

        public IdxOverflow GetOverflow(int tree, int page)
        {
            var overflow = new IdxOverflow(_file.GetPage(page));
            overflow.PageNumber = page;
            return overflow;
        }

        public IdxOverflow CreateOverflow(int tree, int page)
        {
            throw new NotImplementedException();
        }

        public IIdxNode GetNode(int tree, int page)
        {
            return ReadNode(page);
        }

        public IdxLeaf CreateLeaf(int tree)
        {
            throw new NotImplementedException();
        }

        public IdxInterior CreateInterior(int tree)
        {
            throw new NotImplementedException();
        }

        public void Delete(int tree, int page)
        {
        }

        public void SetTreeRoot(int tree, IIdxNode root)
        {
        }

        public void Dispose()
        {
            _file.Dispose();
        }
    }
}
