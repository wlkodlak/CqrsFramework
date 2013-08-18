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
    public class DefinovatPouzivaneNaradiCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public string Vykres { get; set; }
        [DataMember(Order = 2)]
        public string Rozmer { get; set; }
        [DataMember(Order = 3)]
        public string Druh { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class DefinovanoPouzivaneNaradiEvent : IEvent
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public string Vykres { get; set; }
        [DataMember(Order = 2)]
        public string Rozmer { get; set; }
        [DataMember(Order = 3)]
        public string Druh { get; set; }
    }

    public class UpravitPocetNaradiNaSkladeCommand
    {
        public Guid Id { get; set; }
        public int ZmenaMnozstvi { get; set; }
    }

    public class UpravenPocetNaradiNaSkladeEvent : IEvent
    {
        public Guid Id { get; set; }
        public int ZmenaMnozstvi { get; set; }
        public int NoveMnozstvi { get; set; }
    }

    public enum TypUpravyPoctuNaradiNaSklade
    {
        PevnyPocet,
        ZvysitOMnozstvi,
        SnizitOMnozstvi
    }

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

    public interface IPouzivaneNaradiReadService
    {
        SeznamPouzivanehoNaradiDto ZiskatSeznam(int offset, int pocet);
        PouzivaneNaradiDto NajitPodleVykresu(string vykres, string rozmer);
        // PouzivaneNaradiDto NajitPodleId(Guid id);
        // SeznamPouzivanehoNaradiDto HledatVSeznamu(string hledanyVykres, int pocet, int pocetPredNalezenym);
    }

    public interface IPouzivaneNaradiWriteService
    {
        void DefinovatPouzivaneNaradi(DefinovatPouzivaneNaradiCommand cmd);
        void UpravitPocetNaradiNaSklade(UpravitPocetNaradiNaSkladeCommand cmd);
    }

    public class DefinovatPouzivaneNaradiCommandValidator : CommandValidator<DefinovatPouzivaneNaradiCommand>
    {
        public DefinovatPouzivaneNaradiCommandValidator()
        {
            AddRule(ValidationRuleSeverity.Required, "REQ:Vykres", "Výkres je nutné zadat", c => !string.IsNullOrEmpty(c.Vykres));
            AddRule(ValidationRuleSeverity.Required, "REQ:Rozmer", "Rozměr je nutné zadat", c => !string.IsNullOrEmpty(c.Rozmer));
        }
    }
}
