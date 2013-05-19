using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.IndexTable;
using MsAssert = Microsoft.VisualStudio.TestTools.UnitTesting.Assert;

namespace CqrsFramework.Tests.IndexTable
{
    public class IndexTableAssertDirtyChanged
    {
        private IIdxPage _page;
        private bool _dirty = false;

        public IndexTableAssertDirtyChanged(IIdxPage page)
        {
            _page = page;
            _page.IsDirtyChanged += IsDirtyChanged;
        }

        private void IsDirtyChanged(object sender, EventArgs e)
        {
            _dirty = _page.IsDirty;
        }

        public void AssertTrue()
        {
            MsAssert.IsTrue(_dirty);
        }

        public void AssertFalse()
        {
            MsAssert.IsFalse(_dirty);
        }

        public void AssertTrue(string message, params object[] parameters)
        {
            MsAssert.IsTrue(_dirty, message, parameters);
        }

        public void AssertFalse(string message, params object[] parameters)
        {
            MsAssert.IsFalse(_dirty, message, parameters);
        }
    }
}
