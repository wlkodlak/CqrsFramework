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
        public Naradi()
        {
            Register<DefinovanoPouzivaneNaradiEvent>(Apply);
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
        }
    }
}
