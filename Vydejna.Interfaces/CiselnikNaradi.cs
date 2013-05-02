using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vydejna.Interfaces.CiselnikNaradi
{
    public class PouzivaneNaradiDto
    {
        public Guid Id;
        public string Vykres;
        public string Rozmer;
        public string Druh;
        public int PocetAktivnich;
    }

    public class SeznamPouzivanehoNaradiDto : List<PouzivaneNaradiDto>
    {
    }

    public interface ICiselnikNaradiService
    {
        SeznamPouzivanehoNaradiDto ZiskatSeznamNaradi();
        void DefinovatNaradi(DefinovatNaradi cmd);
    }

    public class DefinovatNaradi
    {
        public Guid Id;
        public string Vykres;
        public string Rozmer;
        public string Druh;
    }

    public class DefinovanoNaradi
    {
        public Guid Id;
        public string Vykres;
        public string Rozmer;
        public string Druh;
    }

}
