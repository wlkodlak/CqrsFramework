using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using CqrsFramework.Domain;

namespace Vydejna.Contracts
{
    [DataContract(Namespace = Serialization.Namespace)]
    public class PouzivaneNaradiDto
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public string Vykres { get; set; }
        [DataMember(Order = 2)]
        public string Rozmer { get; set; }
        [DataMember(Order = 3)]
        public string Druh { get; set; }
        [DataMember(Order = 11)]
        public int PocetNaSklade { get; set; }
        [DataMember(Order = 12)]
        public int PocetProVyrobu { get; set; }
        [DataMember(Order = 13)]
        public int PocetProOpravu { get; set; }
        [DataMember(Order = 14)]
        public int PocetProSrot { get; set; }
        [DataMember(Order = 15)]
        public int PocetVeVyrobe { get; set; }
        [DataMember(Order = 16)]
        public int PocetVOprave { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class SeznamPouzivanehoNaradiDto
    {
        [DataMember(Order = 0)]
        public List<PouzivaneNaradiDto> SeznamNaradi { get; set; }
        [DataMember(Order = 1)]
        public int OffsetPrvnihoPrvku { get; set; }
        [DataMember(Order = 2)]
        public int OffsetNalezenehoPrvku { get; set; }
        [DataMember(Order = 3)]
        public int PocetVsechPrvku { get; set; }

        public SeznamPouzivanehoNaradiDto()
        {
            SeznamNaradi = new List<PouzivaneNaradiDto>();
        }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class InformaceOObjednavce
    {
        [DataMember(Order = 0)]
        public string CisloObjednavky { get; set; }
        [DataMember(Order = 1)]
        public Guid Dodavatel { get; set; }
        [DataMember(Order = 2)]
        public DateTime? DatumDodani { get; set; }
        [DataMember(Order = 3)]
        public bool Otevrena { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class UmisteniNaradi
    {
        [DataMember(Order = 0)]
        public OblastUmisteniNaradi Oblast { get; set; }
        [DataMember(Order = 1, EmitDefaultValue = false)]
        public Guid Pracoviste { get; set; }
        [DataMember(Order = 2, EmitDefaultValue = false)]
        public string CisloObjednavky { get; set; }
    }

    public interface IPrehledNaradiReadService
    {
        SeznamPouzivanehoNaradiDto ZiskatSeznam(int offset, int pocet);
        // SeznamPouzivanehoNaradiDto HledatVSeznamu(string hledanyVykres, int pocet, int pocetPredNalezenym);
    }

    public interface IPresunyNaradiReadService
    {
        int PocetKDispozici(UmisteniNaradi umisteni);
        PouzivaneNaradiDto NajitPodleVykresu(string vykres, string rozmer);
        InformaceOObjednavce NajitObjednavku(string cisloObjednavky);
        PouzivaneNaradiDto NajitPodleId(Guid id);
    }
}
