using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace CqrsFramework
{
    public interface IStreamProvider
    {
        Stream Open(string name, FileMode fileMode);
    }
}
