using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using Vydejna.Interfaces.CiselnikNaradi;
using Vydejna.Domain;
using Vydejna.Services;
using Moq;
using Vydejna.Interfaces;

namespace Vydejna.Tests.CiselnikNaradi
{
    [TestClass]
    public class CiselnikNaradiServiceTest
    {
        [TestMethod]
        public void ZiskatNeprazdnySeznamNaradi()
        {
            List<PouzivaneNaradiDto> seznamExistujicich = new List<PouzivaneNaradiDto>();
            seznamExistujicich.Add(new PouzivaneNaradiDto()
            {
                Vykres = "3-26-006",
                Rozmer = "TCMT1102xx",
                Druh = "Držák plátku",
                PocetAktivnich = 0
            });
            seznamExistujicich.Add(new PouzivaneNaradiDto()
            {
                Vykres = "4-41-073",
                Rozmer = "průměr 25/5,8",
                Druh = "Opěrka hlavy",
                PocetAktivnich = 4
            });
            var repositoryMock = new Mock<IPouzivaneNaradiRepository>();
            repositoryMock.Setup(m => m.NajitPouzivaneNaradi()).Returns(seznamExistujicich.ToArray());
            ICiselnikNaradiService service = new CiselnikNaradiService(repositoryMock.Object);
            SeznamPouzivanehoNaradiDto seznam = service.ZiskatSeznamNaradi();
            Assert.AreEqual(2, seznam.Count);
            CollectionAssert.AreEquivalent(seznamExistujicich, seznam.ToList());
        }

        [TestMethod]
        public void DefinovatNoveNaradiPriPrazdnemSeznamu()
        {
            var repositoryMock = new Mock<IPouzivaneNaradiRepository>();
            IEvent[] savedEvents = null;
            var agregat = new SeznamPouzivanehoNaradiAggregate();
            repositoryMock
                .Setup(r => r.NacistAgregat())
                .Returns(agregat);
            repositoryMock
                .Setup(r => r.UlozitAgregat(agregat))
                .Callback<SeznamPouzivanehoNaradiAggregate>(a => savedEvents = (agregat as ICqrsAggregate).GetUncommittedEvents());
            ICiselnikNaradiService service = new CiselnikNaradiService(repositoryMock.Object);
            
            DefinovatNaradi cmd = new DefinovatNaradi();
            cmd.Vykres = "3-26-006";
            cmd.Rozmer = "TCMT1102xx";
            cmd.Druh = "Držák plátku";
            service.DefinovatNaradi(cmd);
            
            DefinovanoNaradi eventDefinovano = savedEvents.Single() as DefinovanoNaradi;
            Assert.AreEqual("3-26-006", eventDefinovano.Vykres);
            Assert.AreEqual("TCMT1102xx", eventDefinovano.Rozmer);
            Assert.AreEqual("Držák plátku", eventDefinovano.Druh);
            
            repositoryMock.VerifyAll();
        }

        [TestMethod]
        public void RozmerADruhNepovinne()
        {
            var repositoryMock = new Mock<IPouzivaneNaradiRepository>();
            IEvent[] savedEvents = null;
            var agregat = new SeznamPouzivanehoNaradiAggregate();
            repositoryMock
                .Setup(r => r.NacistAgregat())
                .Returns(agregat);
            repositoryMock
                .Setup(r => r.UlozitAgregat(agregat))
                .Callback<SeznamPouzivanehoNaradiAggregate>(a => savedEvents = (agregat as ICqrsAggregate).GetUncommittedEvents());
            ICiselnikNaradiService service = new CiselnikNaradiService(repositoryMock.Object);

            DefinovatNaradi cmd = new DefinovatNaradi();
            cmd.Vykres = "3-26-006";
            cmd.Rozmer = null;
            cmd.Druh = null;
            service.DefinovatNaradi(cmd);

            DefinovanoNaradi eventDefinovano = savedEvents.Single() as DefinovanoNaradi;
            Assert.AreEqual("3-26-006", eventDefinovano.Vykres);
            Assert.AreEqual(null, eventDefinovano.Rozmer);
            Assert.AreEqual(null, eventDefinovano.Druh);
            
            repositoryMock.VerifyAll();
        }

        [TestMethod]
        public void VykresJeNutneZadat()
        {
            try
            {
                var repositoryMock = new Mock<IPouzivaneNaradiRepository>();
                var agregat = new SeznamPouzivanehoNaradiAggregate();
                repositoryMock
                    .Setup(r => r.NacistAgregat())
                    .Returns(agregat);

                ICiselnikNaradiService service = new CiselnikNaradiService(repositoryMock.Object);

                DefinovatNaradi cmd = new DefinovatNaradi();
                cmd.Vykres = null;
                cmd.Rozmer = "TCMT1102xx";
                cmd.Druh = "Držák plátku";
                service.DefinovatNaradi(cmd);
                
                Assert.Fail("Ocekavana vyjimka");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        [TestMethod]
        public void KombinaceVykresuARozmeruMusiBytUnikatni()
        {
            
        }
    }
}
