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
    public class UpravitPocetNaradiNaSkladeCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int ZmenaMnozstvi { get; set; }
        [DataMember(Order = 2)]
        public TypUpravyPoctuNaradiNaSklade TypUpravy { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class PrijmoutNaradiZeSkladuCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
        [DataMember(Order = 2)]
        public Guid Dodavatel { get; set; }
        [DataMember(Order = 3)]
        public decimal Cena { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class VydatNaradiDoVyrobyCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
        [DataMember(Order = 3)]
        public decimal? Cena { get; set; }
        [DataMember(Order = 6)]
        public string Pracoviste { get; set; }
        [DataMember(Order = 8)]
        public bool OkamzitaSpotreba { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class PrijmoutNaradiZVyrobyCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
        [DataMember(Order = 3)]
        public decimal? Cena { get; set; }
        [DataMember(Order = 6)]
        public string Pracoviste { get; set; }
        [DataMember(Order = 7)]
        public StavPrijatehoNaradi StavNaradi { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class VydatNaradiNaOpravuCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
        [DataMember(Order = 2)]
        public Guid Dodavatel { get; set; }
        [DataMember(Order = 3)]
        public decimal? Cena { get; set; }
        [DataMember(Order = 8)]
        public bool Reklamace { get; set; }
        [DataMember(Order = 9)]
        public string CisloObjednavky { get; set; }
        [DataMember(Order = 10)]
        public DateTime TerminDodani { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class PrijmoutNaradiZOpravyCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
        [DataMember(Order = 2)]
        public Guid Dodavatel { get; set; }
        [DataMember(Order = 3)]
        public decimal? Cena { get; set; }
        [DataMember(Order = 8)]
        public bool Reklamace { get; set; }
        [DataMember(Order = 9)]
        public string CisloObjednavky { get; set; }
        [DataMember(Order = 10)]
        public string CisloDodacihoListu { get; set; }
        [DataMember(Order = 12)]
        public StavPrijatehoNaradi StavNaradi { get; set; }
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public class VydatNaradiDoSrotuCommand
    {
        [DataMember(Order = 0)]
        public Guid Id { get; set; }
        [DataMember(Order = 1)]
        public int Mnozstvi { get; set; }
    }

    public interface INaradiWriteService
    {
        void DefinovatPouzivaneNaradi(DefinovatPouzivaneNaradiCommand cmd);
        void UpravitPocetNaradiNaSklade(UpravitPocetNaradiNaSkladeCommand cmd);
        void PrijmoutNaradiZeSkladu(PrijmoutNaradiZeSkladuCommand cmd);
        //void PrijmoutNaradiZVyroby(PrijmoutNaradiZVyrobyCommand cmd);
        //void PrijmoutNaradiZOpravy(PrijmoutNaradiZOpravyCommand cmd);
        //void VydatNaradiDoVyroby(VydatNaradiDoVyrobyCommand cmd);
        //void VydatNaradiNaOpravu(VydatNaradiNaOpravuCommand cmd);
        //void VydatNaradiDoSrotu(VydatNaradiDoSrotuCommand cmd);
    }

}
