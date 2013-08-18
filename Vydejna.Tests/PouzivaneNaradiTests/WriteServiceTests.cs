using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using Vydejna.Domain;
using CqrsFramework.Domain;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Vydejna.Tests.PouzivaneNaradiTests
{
    [TestClass]
    public class WriteServiceTests
    {
        [TestMethod]
        public void UspesneDefinovatNaradi()
        {
            var repo = new Mock<IRepository<Guid, Naradi>>();
            Naradi naradi = null;
            repo.Setup(r => r.Save(It.IsAny<Naradi>(), It.IsAny<object>(), It.IsAny<RepositorySaveFlags>()))
                .Callback<Naradi, object, RepositorySaveFlags>((n, c, f) => naradi = n);
            var cmd = new DefinovatPouzivaneNaradiCommand();
            cmd.Id = new Guid("C3DD077F-3260-4780-B477-F9282540204E");
            cmd.Vykres = "vykres-01";
            cmd.Rozmer = "rozmer-01.a";
            cmd.Druh = "A-kategorie";
            var svc = new PouzivaneNaradiWriteService(repo.Object);
            svc.DefinovatPouzivaneNaradi(cmd);
            Assert.IsNotNull(naradi, "Naradi nebylo ulozeno");
            var events = (naradi as IAggregate).GetEvents().ToArray();
            Assert.AreEqual(1, events.Length, "Pocet eventu");
            Assert.IsInstanceOfType(events[0], typeof(DefinovanoPouzivaneNaradiEvent), "Typ eventu");
            var event0 = (DefinovanoPouzivaneNaradiEvent)events[0];
            Assert.AreEqual(cmd.Id, event0.Id, "event.Id");
            Assert.AreEqual(cmd.Vykres, event0.Vykres, "event.Vykres");
            Assert.AreEqual(cmd.Rozmer, event0.Rozmer, "event.Rozmer");
            Assert.AreEqual(cmd.Druh, event0.Druh, "event.Druh");
        }

        [TestMethod]
        public void ChybaValidacePrikazuDefiniceNaradi()
        {
            var repo = new Mock<IRepository<Guid, Naradi>>();
            Naradi naradi = null;
            repo.Setup(r => r.Save(It.IsAny<Naradi>(), It.IsAny<object>(), It.IsAny<RepositorySaveFlags>()))
                .Callback<Naradi, object, RepositorySaveFlags>((n, c, f) => naradi = n);
            var cmd = new DefinovatPouzivaneNaradiCommand();
            cmd.Vykres = "";
            cmd.Rozmer = "rozmer-01.a";
            cmd.Druh = "";
            var svc = new PouzivaneNaradiWriteService(repo.Object);
            try
            {
                svc.DefinovatPouzivaneNaradi(cmd);
                Assert.Fail("Ocekavana vyjimka DomainErrorException");
            }
            catch (DomainErrorException)
            {
                Assert.IsNull(naradi);
            }
        }

        [TestMethod]
        public void ValidacePrikazuDefiniceNaradi()
        {
            var cmd = new DefinovatPouzivaneNaradiCommand();
            cmd.Vykres = "";
            cmd.Rozmer = "rozmer-01.a";
            cmd.Druh = "";

            var validator = new DefinovatPouzivaneNaradiCommandValidator();
            var result = validator.Validate(cmd);
            Assert.AreEqual(ValidationRuleSeverity.Required, result.Severity, "Severity");
            Assert.AreEqual(1, result.BrokenRules.Count, "Broken count");
            Assert.AreEqual("REQ:Vykres", result.BrokenRules[0].ErrorCode, "ErrorCode[0]");
        }

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
        public void UspesneZmenitStavNaSklade()
        {
            var repo = new Mock<IRepository<Guid, Naradi>>();
            var id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1");
            var naradi = new Naradi();
            (naradi as IAggregate).LoadFromHistory(null, new IEvent[]
            {
                new DefinovanoPouzivaneNaradiEvent()
                {
                    Id = id, Vykres = "vykres", Rozmer = "rozmer", Druh = "kategorie"
                },
                new UpravenPocetNaradiNaSkladeEvent()
                {
                    Id = id, TypUpravy = TypUpravyPoctuNaradiNaSklade.PevnyPocet, ZmenaMnozstvi = 2, NoveMnozstvi = 2
                }
            });
            var cmd = new UpravitPocetNaradiNaSkladeCommand()
            {
                Id = id,
                TypUpravy = TypUpravyPoctuNaradiNaSklade.ZvysitOMnozstvi,
                ZmenaMnozstvi = 3
            };
            repo.Setup(r => r.Get(id)).Returns(naradi).Verifiable();
            repo.Setup(r => r.Save(naradi, It.IsAny<object>(), It.IsAny<RepositorySaveFlags>())).Verifiable();
            var svc = new PouzivaneNaradiWriteService(repo.Object);
            svc.UpravitPocetNaradiNaSklade(cmd);
            repo.Verify();
            var newEvents = (naradi as IAggregate).GetEvents().ToArray();
            Assert.IsInstanceOfType(newEvents[0], typeof(UpravenPocetNaradiNaSkladeEvent));
            Assert.AreEqual(5, (newEvents[0] as UpravenPocetNaradiNaSkladeEvent).NoveMnozstvi);
        }

        [TestMethod]
        public void NevalidniPokusOZmenuStavuNaradi()
        {
            var repo = new Mock<IRepository<Guid, Naradi>>();
            var id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1");
            var cmd = new UpravitPocetNaradiNaSkladeCommand()
            {
                Id = id,
                TypUpravy = TypUpravyPoctuNaradiNaSklade.ZvysitOMnozstvi,
                ZmenaMnozstvi = -3
            };
            var svc = new PouzivaneNaradiWriteService(repo.Object);
            try
            {
                svc.UpravitPocetNaradiNaSklade(cmd);
                Assert.Fail("Ocekavana chyba validace");
            }
            catch (DomainErrorException)
            {
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
