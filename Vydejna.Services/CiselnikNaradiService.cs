using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Domain;
using Vydejna.Interfaces.CiselnikNaradi;

namespace Vydejna.Services
{
    public class CiselnikNaradiService : ICiselnikNaradiService
    {
        private IPouzivaneNaradiRepository _repository;

        public CiselnikNaradiService(IPouzivaneNaradiRepository repository)
        {
            _repository = repository;
        }

        public SeznamPouzivanehoNaradiDto ZiskatSeznamNaradi()
        {
            var seznam = _repository.NajitPouzivaneNaradi();
            var dto = new SeznamPouzivanehoNaradiDto();
            dto.AddRange(seznam);
            return dto;
        }


        public void DefinovatNaradi(DefinovatNaradi cmd)
        {
            var seznam = _repository.NacistAgregat();
            seznam.DefinovatNaradi(cmd.Id, cmd.Vykres, cmd.Rozmer, cmd.Druh);
            _repository.UlozitAgregat(seznam);
        }
    }
}
