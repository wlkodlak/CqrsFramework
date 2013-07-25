using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vydejna.Contracts
{
    public interface ISeznamPouzivanehoNaradiService
    {
        PolozkaSeznamuPouzivanehoNaradi[] ZiskatSeznam();
    }

    public class PolozkaSeznamuPouzivanehoNaradi
    {
        public Guid Id { get; set; }
        public string Vykres { get; set; }
        public string Rozmer { get; set; }
        public string Druh { get; set; }
    }
}
