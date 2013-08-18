using CqrsFramework.Domain;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using Vydejna.Domain;

namespace Vydejna.Tests.PouzivaneNaradiWriteServiceTests
{
    [TestClass]
    public class UpravitStavNaSkladeTest
    {
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
    }
}
