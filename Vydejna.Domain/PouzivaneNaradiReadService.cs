using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using CqrsFramework.Messaging;

namespace Vydejna.Domain
{
    public class PouzivaneNaradiReadService : IPouzivaneNaradiReadService
    {
        private IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto> _docs;

        public PouzivaneNaradiReadService(IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto> docs)
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

        public SeznamPouzivanehoNaradiDto ZiskatSeznam(int offset, int pocet)
        {
            var dto = new SeznamPouzivanehoNaradiDto();
            var zaklad = _docs.Get("prehlednaradi-vsechno");
            dto.SeznamNaradi = new List<PouzivaneNaradiDto>(zaklad.SeznamNaradi.Skip(offset).Take(pocet));
            dto.OffsetPrvnihoPrvku = offset;
            dto.PocetVsechPrvku = zaklad.PocetVsechPrvku;
            return dto;
        }
    }
}
