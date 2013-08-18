using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using Vydejna.Domain;
using Moq;

namespace Vydejna.Tests.PouzivaneNaradiTests
{
    [TestClass]
    public class ReadServiceTests
    {
        [TestMethod]
        public void ZiskatPrazdnySeznamPouzivanehoNaradi()
        {
            var zaklad = new SeznamPouzivanehoNaradiDto() { SeznamNaradi = new List<PouzivaneNaradiDto>() };
            var docs = new Mock<CqrsFramework.Messaging.IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto>>();
            docs.Setup(s => s.Get("prehlednaradi-vsechno")).Returns(zaklad).Verifiable();
            var svc = new PouzivaneNaradiReadService(docs.Object);
            var seznam = svc.ZiskatSeznam(0, int.MaxValue);
            docs.Verify();
            Assert.AreEqual(0, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(0, seznam.SeznamNaradi.Count, "Pocet v kolekci");
        }

        [TestMethod]
        public void ZiskatNaplnenySeznamVsehoPouzivanehoNaradi()
        {
            var zaklad = VygenerovatZakladSeznamuVsehoNaradi();
            var docs = new Mock<CqrsFramework.Messaging.IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto>>();
            docs.Setup(s => s.Get("prehlednaradi-vsechno")).Returns(zaklad).Verifiable();
            var svc = new PouzivaneNaradiReadService(docs.Object);
            var seznam = svc.ZiskatSeznam(0, int.MaxValue);
            docs.Verify();
            Assert.AreEqual(5, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(5, seznam.SeznamNaradi.Count, "Pocet v kolekci");
        }

        private static SeznamPouzivanehoNaradiDto VygenerovatZakladSeznamuVsehoNaradi()
        {
            var zaklad = new SeznamPouzivanehoNaradiDto();
            zaklad.SeznamNaradi = new List<PouzivaneNaradiDto>();
            zaklad.SeznamNaradi.Add(new PouzivaneNaradiDto()
            {
                Vykres = "vykres-1",
                Rozmer = "rozmer-1a",
                Druh = "kategorie01",
                Id = Guid.Parse("0B9EC0EC-B017-4FF5-8C69-DE9DBB66820A")
            });
            zaklad.SeznamNaradi.Add(new PouzivaneNaradiDto()
            {
                Vykres = "vykres-2",
                Rozmer = "rozmer-2a",
                Druh = "kategorie01",
                Id = Guid.Parse("319CF496-8CEA-4133-BB21-D3DD9A333060")
            });
            zaklad.SeznamNaradi.Add(new PouzivaneNaradiDto()
            {
                Vykres = "vykres-2",
                Rozmer = "rozmer-2b",
                Druh = "kategorie01",
                Id = Guid.Parse("CAC7FF44-E669-4B81-B9F9-79DD830C0250")
            });
            zaklad.SeznamNaradi.Add(new PouzivaneNaradiDto()
            {
                Vykres = "vykres-3",
                Rozmer = "rozmer-3c",
                Druh = "kategorie06",
                Id = Guid.Parse("B65C9239-DEE8-4E05-8650-EF95160DE36F")
            });
            zaklad.SeznamNaradi.Add(new PouzivaneNaradiDto()
            {
                Vykres = "vykres-4",
                Rozmer = "rozmer-4a",
                Druh = "kategorie00",
                Id = Guid.Parse("EAFFD9B6-E30A-44EF-A762-73082ADFD1D2")
            });
            zaklad.PocetVsechPrvku = zaklad.SeznamNaradi.Count;
            return zaklad;
        }

        [TestMethod]
        public void ZiskatCastNaplnenehoSeznamuVsehoNaradi()
        {
            var zaklad = VygenerovatZakladSeznamuVsehoNaradi();
            var docs = new Mock<CqrsFramework.Messaging.IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto>>();
            docs.Setup(s => s.Get("prehlednaradi-vsechno")).Returns(zaklad).Verifiable();
            var svc = new PouzivaneNaradiReadService(docs.Object);
            var seznam = svc.ZiskatSeznam(1, 2);
            docs.Verify();
            Assert.AreEqual(5, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(2, seznam.SeznamNaradi.Count, "Pocet v kolekci");
            Assert.AreEqual(1, seznam.OffsetPrvnihoPrvku, "Offset");
            Assert.AreEqual(zaklad.SeznamNaradi[1].Id, seznam.SeznamNaradi[0].Id, "Id[0]");
            Assert.AreEqual(zaklad.SeznamNaradi[2].Id, seznam.SeznamNaradi[1].Id, "Id[1]");
        }

        [TestMethod]
        public void OveritExistenciPouzivaneNaradiPodleVykresu()
        {
            var zaklad = VygenerovatZakladSeznamuVsehoNaradi();
            var docs = new Mock<CqrsFramework.Messaging.IKeyValueProjectionReader<SeznamPouzivanehoNaradiDto>>();
            docs.Setup(s => s.Get("prehlednaradi-vsechno")).Returns(zaklad).Verifiable();
            var svc = new PouzivaneNaradiReadService(docs.Object);
            var nalezenyVykres0 = svc.NajitPodleVykresu("vykres-0", "rozmer-0a");
            var nalezenyVykres2 = svc.NajitPodleVykresu("vykres-2", "rozmer-2b");
            docs.Verify();
            Assert.IsNull(nalezenyVykres0, "vykres-0");
            Assert.IsNotNull(nalezenyVykres2, "vykres-2");
            Assert.AreEqual(Guid.Parse("CAC7FF44-E669-4B81-B9F9-79DD830C0250"), nalezenyVykres2.Id, "vykres-2: id");
        }
    }
}
