using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using CqrsFramework.Messaging;

namespace Vydejna.Domain
{
    public class PresunyNaradiReadService : IPresunyNaradiReadService
    {
        private IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto> _docs;

        public PresunyNaradiReadService(IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto> docs)
        {
            _docs = docs;
        }


        public PouzivaneNaradiDto NajitPodleVykresu(string vykres, string rozmer)
        {
            var zaklad = _docs.Get("prehlednaradi-vsechno");
            var nalezene = zaklad.SeznamNaradi.Where(n => NaradiOdpovidaVykresuARozmeru(vykres, rozmer, n)).FirstOrDefault();
            return nalezene;
        }

        private static bool NaradiOdpovidaVykresuARozmeru(string vykres, string rozmer, PouzivaneNaradiDto n)
        {
            return string.Equals(vykres, n.Vykres, StringComparison.OrdinalIgnoreCase)
                && string.Equals(rozmer, n.Rozmer, StringComparison.OrdinalIgnoreCase);
        }

        public PouzivaneNaradiDto NajitPodleId(Guid id)
        {
            var zaklad = _docs.Get("prehlednaradi-vsechno");
            var nalezene = zaklad.SeznamNaradi.FirstOrDefault(n => id == n.Id);
            return nalezene;
        }


        public int PocetKDispozici(UmisteniNaradi umisteni)
        {
            throw new NotImplementedException();
        }

        public InformaceOObjednavce NajitObjednavku(string cisloObjednavky)
        {
            throw new NotImplementedException();
        }
    }
}
