using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace Vydejna.Contracts
{
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

    [DataContract(Namespace = Serialization.Namespace)]
    public enum StavPrijatehoNaradi
    {
        ProVyrobu,
        ProOpravu,
        ProSrot
    }

    [DataContract(Namespace = Serialization.Namespace)]
    public enum OblastUmisteniNaradi
    {
        ProVyrobu, ProOpravu, ProSrot,
        VeVyrobe, VOprave
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

    public interface IPresunyNaradiReadService
    {
        int PocetKDispozici(UmisteniNaradi umisteni);
        InformaceOObjednavce NajitObjednavku(string cisloObjednavky);
    }

    public interface IPresunyNaradiWriteService
    {
        void PrijmoutNaradiZeSkladuCommand(PrijmoutNaradiZeSkladuCommand cmd);
        void PrijmoutNaradiZVyrobyCommand(PrijmoutNaradiZVyrobyCommand cmd);
        void PrijmoutNaradiZOpravyCommand(PrijmoutNaradiZOpravyCommand cmd);
        void VydatNaradiDoVyrobyCommand(VydatNaradiDoVyrobyCommand cmd);
        void VydatNaradiNaOpravuCommand(VydatNaradiNaOpravuCommand cmd);
        void VydatNaradiDoSrotuCommand(VydatNaradiDoSrotuCommand cmd);
    }
}
