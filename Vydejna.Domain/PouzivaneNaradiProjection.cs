using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Messaging;
using Vydejna.Contracts;
using CqrsFramework.KeyValueStore;

namespace Vydejna.Domain
{
    public class PouzivaneNaradiProjection : KeyValueProjection<SeznamPouzivanehoNaradiDto>
    {
        public PouzivaneNaradiProjection(IKeyValueStore store, IKeyValueProjectionStrategy strategy)
            : base(store, strategy, "prehlednaradi", new byte[] { 0x33, 0x12, 0x23, 0x34 })
        {
            Register<DefinovanoPouzivaneNaradiEvent>(e => "prehlednaradi-vsechno", Handle);
        }

        private void Handle(DefinovanoPouzivaneNaradiEvent ev, SeznamPouzivanehoNaradiDto view)
        {
            var item = new PouzivaneNaradiDto();
            item.Id = ev.Id;
            item.Vykres = ev.Vykres;
            item.Rozmer = ev.Rozmer;
            item.Druh = ev.Druh;
            view.SeznamNaradi.Add(item);
            view.PocetVsechPrvku = view.SeznamNaradi.Count;
        }
    }
}
