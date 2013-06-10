using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework.Infrastructure
{
    public interface IStreamProvider
    {
        Stream Open(string name, FileMode fileMode);
        IEnumerable<string> GetStreams();
        void Delete(string name);
    }
}
