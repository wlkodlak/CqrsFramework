using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using CqrsFramework.Domain;

namespace Vydejna.Contracts
{
    public class DefinovatPouzivaneNaradiCommandValidator : CommandValidator<DefinovatPouzivaneNaradiCommand>
    {
        public DefinovatPouzivaneNaradiCommandValidator(IPrehledNaradiReadService readSvc = null)
        {
            AddRule(ValidationRuleSeverity.Required, "REQ:Vykres", "Výkres je nutné zadat", c => !string.IsNullOrEmpty(c.Vykres));
            AddRule(ValidationRuleSeverity.Required, "REQ:Rozmer", "Rozměr je nutné zadat", c => !string.IsNullOrEmpty(c.Rozmer));
            if (readSvc != null)
            {
                AddRule(ValidationRuleSeverity.Error, "CONFLICT:Vykres+Rozmer", "Nářadí již existuje",
                    c => !readSvc.ExistujeVykresARozmer(c.Vykres, c.Rozmer));
            }
        }
    }

    public class UpravitPocetNaradiNaSkladeCommandValidator : CommandValidator<UpravitPocetNaradiNaSkladeCommand>
    {
        public UpravitPocetNaradiNaSkladeCommandValidator(IPresunyNaradiReadService readSvc = null)
        {
            AddRule(ValidationRuleSeverity.Error, "RANGE:ZmenaMnozstvi", "Množství nesmí být záporné",
                c => c.ZmenaMnozstvi >= 0 || c.TypUpravy != TypUpravyPoctuNaradiNaSklade.PevnyPocet);
            AddRule(ValidationRuleSeverity.Error, "RANGE:ZmenaMnozstvi", "Množství pro relativní změnu musí být kladné",
                c => c.ZmenaMnozstvi > 0 || c.TypUpravy == TypUpravyPoctuNaradiNaSklade.PevnyPocet);
            if (readSvc != null)
            {
                AddRule(ValidationRuleSeverity.Error, "RANGE:ZmenaMnozstvi", "Množství pro snížení nesmí překročit stav na skladě",
                    c =>
                    {
                        if (c.TypUpravy != TypUpravyPoctuNaradiNaSklade.SnizitOMnozstvi)
                            return true;
                        var naradi = readSvc.NajitPodleId(c.Id);
                        if (naradi == null)
                            return true;
                        return naradi.PocetNaSklade > c.ZmenaMnozstvi;
                    });
            }
        }
    }
}
