using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Domain;
using Vydejna.Contracts;

namespace Vydejna.Domain
{
    public class Naradi : AggregateBase
    {
        private Guid _id;
        private int _pocetNaSklade;

        public Naradi()
        {
            Register<DefinovanoPouzivaneNaradiEvent>(Apply);
            Register<UpravenPocetNaradiNaSkladeEvent>(Apply);
        }

        public void Definovat(Guid id, string vykres, string rozmer, string druh)
        {
            Publish(new DefinovanoPouzivaneNaradiEvent()
            {
                Id = id,
                Vykres = vykres,
                Rozmer = rozmer,
                Druh = druh
            });
        }

        private void Apply(DefinovanoPouzivaneNaradiEvent ev)
        {
            _id = ev.Id;
        }

        public void UpravitPocetNaSklade(TypUpravyPoctuNaradiNaSklade typ, int pocet)
        {
            Publish(new UpravenPocetNaradiNaSkladeEvent()
            {
                Id = _id,
                TypUpravy = typ,
                ZmenaMnozstvi = pocet,
                NoveMnozstvi = _pocetNaSklade + pocet
            });
        }

        private void Apply(UpravenPocetNaradiNaSkladeEvent ev)
        {
            _pocetNaSklade = ev.NoveMnozstvi;
        }
    }
}
