using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using CqrsFramework.Domain;
using Vydejna.Contracts;
using Vydejna.Domain;

namespace Vydejna.Tests.PouzivaneNaradiWriteServiceTests
{
    [TestClass]
    public class PrijmoutNaradiZeSkladuTest : NaradiTestBase
    {
        [TestMethod]
        public void PrijmoutNoveZeSkladu()
        {
            PripravitService(new IEvent[] { EvDefiniceNaradi(), EvUpravitSklad(4) });
            Service.PrijmoutNaradiZeSkladu(new PrijmoutNaradiZeSkladuCommand
            {
                Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                Mnozstvi = 3,
                Dodavatel = new Guid("D8BB24DB-3749-A329-2BA9-9DC4687B93A0"),
                Cena = 142.5m
            });
            Assert.AreEqual(1, UlozeneUdalosti.Count, "Pocet eventu");
            var ev = UlozeneUdalosti[0] as PrijatoNaradiZeSkladuEvent;
            Assert.AreEqual(3, ev.Mnozstvi, "Mnozstvi");
        }

        private DefinovanoPouzivaneNaradiEvent EvDefiniceNaradi()
        {
            return new DefinovanoPouzivaneNaradiEvent
            {
                Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                Druh = "kategorie",
                Rozmer = "rozmer-2a",
                Vykres = "vykres-1"
            };
        }

        private UpravenPocetNaradiNaSkladeEvent EvUpravitSklad(int pocet)
        {
            return new UpravenPocetNaradiNaSkladeEvent
            {
                Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                TypUpravy = TypUpravyPoctuNaradiNaSklade.PevnyPocet,
                ZmenaMnozstvi = pocet,
                NoveMnozstvi = pocet
            };
        }

        [TestMethod]
        public void NedostatekNaSklade()
        {
            try
            {
                PripravitService(new IEvent[] { EvDefiniceNaradi(), EvUpravitSklad(2) });
                Service.PrijmoutNaradiZeSkladu(new PrijmoutNaradiZeSkladuCommand
                {
                    Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                    Mnozstvi = 3,
                    Dodavatel = new Guid("D8BB24DB-3749-A329-2BA9-9DC4687B93A0"),
                    Cena = 142.5m
                });
                Assert.Fail("Ocekavana chyba - nedostatek na sklade");
            }
            catch (DomainErrorException)
            {
            }
        }

        [TestMethod]
        public void PoPrijetiSeSniziPocetNaSklade()
        {
            PripravitService(new IEvent[] { EvDefiniceNaradi(), EvUpravitSklad(4) });
            Service.PrijmoutNaradiZeSkladu(new PrijmoutNaradiZeSkladuCommand
            {
                Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                Mnozstvi = 3,
                Dodavatel = new Guid("D8BB24DB-3749-A329-2BA9-9DC4687B93A0"),
                Cena = 142.5m
            });
            try
            {
                Service.PrijmoutNaradiZeSkladu(new PrijmoutNaradiZeSkladuCommand
                {
                    Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                    Mnozstvi = 3,
                    Dodavatel = new Guid("D8BB24DB-3749-A329-2BA9-9DC4687B93A0"),
                    Cena = 142.5m
                });
                Assert.Fail("Ocekavana chyba - nedostatek na sklade");
            }
            catch (DomainErrorException)
            {
            }
        }

        [TestMethod]
        public void ChybaValidace()
        {
            try
            {
                PripravitService(new IEvent[] { EvDefiniceNaradi(), EvUpravitSklad(4) });
                Service.PrijmoutNaradiZeSkladu(new PrijmoutNaradiZeSkladuCommand
                {
                    Id = new Guid("B4D600FE-F446-4B25-A88A-C6EB781B5EDD"),
                    Mnozstvi = -3,
                    Dodavatel = Guid.Empty,
                    Cena = 0
                });
                Assert.Fail("Ocekavana chyba validace");
            }
            catch (ValidationErrorException)
            {
            }
        }
    }
}
