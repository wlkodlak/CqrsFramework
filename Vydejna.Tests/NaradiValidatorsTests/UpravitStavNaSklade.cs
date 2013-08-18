using CqrsFramework.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;

namespace Vydejna.Tests.NaradiValidatorsTests
{
    [TestClass]
    public class UpravitStavNaSklade
    {
        [TestMethod]
        public void ValidacePrikazuZmenyPoctuNaSklade()
        {
            var validator = new UpravitPocetNaradiNaSkladeCommandValidator();

            {
                var result = validator.Validate(new UpravitPocetNaradiNaSkladeCommand()
                {
                    Id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1"),
                    ZmenaMnozstvi = -2,
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.PevnyPocet
                });
                Assert.AreEqual(1, result.BrokenRules.Count, "Zaporne mnozstvi (pocet chyb)");
                Assert.AreEqual("RANGE:ZmenaMnozstvi", result.BrokenRules[0].ErrorCode, "Zaporne mnozstvi");
            }
            {
                var result = validator.Validate(new UpravitPocetNaradiNaSkladeCommand()
                {
                    Id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1"),
                    ZmenaMnozstvi = 0,
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.SnizitOMnozstvi
                });
                Assert.AreEqual(1, result.BrokenRules.Count, "Nula pro zmenu (pocet chyb)");
                Assert.AreEqual("RANGE:ZmenaMnozstvi", result.BrokenRules[0].ErrorCode, "Nula pro zmenu");
            }
            {
                var result = validator.Validate(new UpravitPocetNaradiNaSkladeCommand()
                {
                    Id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1"),
                    ZmenaMnozstvi = 2,
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.ZvysitOMnozstvi
                });
                Assert.AreEqual(ValidationRuleSeverity.NoError, result.Severity, "Bez chyby");
            }
        }

        [TestMethod]
        public void KontrolaDostatkuNaradiNaSkladeProSnizeni()
        {
            var id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1");
            var svc = new Mock<IPresunyNaradiReadService>();
            svc.Setup(s => s.NajitPodleId(id)).Returns(new PouzivaneNaradiDto()
            {
                Id = id,
                PocetNaSklade = 2
            }).Verifiable();
            var validator = new UpravitPocetNaradiNaSkladeCommandValidator(svc.Object);
            {
                var cmd = new UpravitPocetNaradiNaSkladeCommand()
                {
                    Id = id,
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.SnizitOMnozstvi,
                    ZmenaMnozstvi = 3
                };
                var result = validator.Validate(cmd);
                Assert.AreEqual(1, result.BrokenRules.Count, "Pocet chyb");
                Assert.AreEqual("RANGE:ZmenaMnozstvi", result.BrokenRules[0].ErrorCode, "Typ chyby");
            }
            {
                var cmd = new UpravitPocetNaradiNaSkladeCommand()
                {
                    Id = id,
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.SnizitOMnozstvi,
                    ZmenaMnozstvi = 1
                };
                var result = validator.Validate(cmd);
                Assert.AreEqual(0, result.BrokenRules.Count, "Pocet chyb");
            }
            svc.Verify();
        }
    }
}
