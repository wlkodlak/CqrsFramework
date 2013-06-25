using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Messaging
{
    public class KeyValueProjection : IProjectionDispatcher
    {
        public KeyValueProjection()
        {
            throw new NotImplementedException();
        }

        public void BeginUpdate()
        {
            throw new NotImplementedException();
        }

        public void EndUpdate()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public bool NeedsRebuild()
        {
            throw new NotImplementedException();
        }

        public long GetClockToHandle()
        {
            throw new NotImplementedException();
        }

        public void Dispatch(Message message)
        {
            throw new NotImplementedException();
        }
    }
}
