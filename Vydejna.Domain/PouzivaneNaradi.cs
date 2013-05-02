using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Interfaces.CiselnikNaradi;

namespace Vydejna.Domain
{
    public interface IPouzivaneNaradiRepository
    {
        PouzivaneNaradiDto[] NajitPouzivaneNaradi();
        SeznamPouzivanehoNaradiAggregate NacistAgregat();
        void UlozitAgregat(SeznamPouzivanehoNaradiAggregate agregat);
    }

    public class SeznamPouzivanehoNaradiAggregate
    {
        public void DefinovatNaradi(Guid id, string vykres, string rozmer, string druh)
        {
            
        }
    }
}
