using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.IndexTable
{
    public interface IIdxPage
    {
        int PageNumber { get; set; }
        bool IsDirty { get; }
        byte[] Save();
        event EventHandler IsDirtyChanged;
    }

    public interface IIdxNode : IIdxPage
    {
        bool IsLeaf { get; }
        int CellsCount { get; }
        IdxCell GetCell(int index);
    }

    public abstract class IdxPageBase : IIdxPage
    {
        private int _pageNumber;
        private bool _dirty = false;

        public int PageNumber
        {
            get { return _pageNumber; }
            set { _pageNumber = value; }
        }

        public bool IsDirty
        {
            get { return _dirty; }
        }

        protected void SetDirty(bool dirty)
        {
            if (_dirty != dirty)
            {
                _dirty = dirty;
                if (IsDirtyChanged != null)
                    IsDirtyChanged(this, EventArgs.Empty);
            }
        }

        public event EventHandler IsDirtyChanged;
        public abstract byte[] Save();
    }
}
