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
    public class UpravitStavNaSkladeTest : NaradiTestBase
    {
        [TestMethod]
        public void UspesneZmenitStavNaSklade()
        {
            var id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1");
            PripravitService(new IEvent[]
            {
                new DefinovanoPouzivaneNaradiEvent()
                {
                    Id = id, 
                    Vykres = "vykres", 
                    Rozmer = "rozmer", 
                    Druh = "kategorie"
                },
                new UpravenPocetNaradiNaSkladeEvent()
                {
                    Id = id, 
                    TypUpravy = TypUpravyPoctuNaradiNaSklade.PevnyPocet, 
                    ZmenaMnozstvi = 2, 
                    NoveMnozstvi = 2
                }
            });
            var cmd = new UpravitPocetNaradiNaSkladeCommand()
            {
                Id = id,
                TypUpravy = TypUpravyPoctuNaradiNaSklade.ZvysitOMnozstvi,
                ZmenaMnozstvi = 3
            };
            Service.UpravitPocetNaradiNaSklade(cmd);
            Assert.IsInstanceOfType(UlozeneUdalosti[0], typeof(UpravenPocetNaradiNaSkladeEvent));
            Assert.AreEqual(5, (UlozeneUdalosti[0] as UpravenPocetNaradiNaSkladeEvent).NoveMnozstvi);
        }

        [TestMethod]
        public void NevalidniPokusOZmenuStavuNaradi()
        {
            var id = new Guid("AC3862E8-DB76-4190-884D-05E00355F1F1");
            var cmd = new UpravitPocetNaradiNaSkladeCommand()
            {
                Id = id,
                TypUpravy = TypUpravyPoctuNaradiNaSklade.ZvysitOMnozstvi,
                ZmenaMnozstvi = -3
            };
            try
            {
                PripravitService(null);
                Service.UpravitPocetNaradiNaSklade(cmd);
                Assert.Fail("Ocekavana chyba validace");
            }
            catch (DomainErrorException)
            {
            }
        }
    }
}
