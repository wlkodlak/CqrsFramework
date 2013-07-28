using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using Vydejna.Domain;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;
using CqrsFramework.KeyValueStore;
using Moq;

namespace Vydejna.Tests.SeznamPouzivanehoNaradiTests
{
    [TestClass]
    public class SeznamPouzivanehoNaradiServiceTest
    {
        [TestMethod]
        public void NacteniSeznamuPouzivanehoNaradi()
        {
            var view = new SeznamPouzivanehoNaradi();
            view.Seznam = new List<PolozkaSeznamuPouzivanehoNaradi>()
            {
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="5847-5877", Rozmer="80x40x20", Druh="Druh naradi"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="2144-5345", Rozmer="120x50x20", Druh="Klestina"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="3481-7484", Rozmer="800x20", Druh="Brusny papir"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="22-254-51", Rozmer="175x35x5", Druh="Datelna"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="2487-5448", Rozmer="defektni", Druh="Spacek"}
            };

            var projection = new Mock<IKeyValueProjectionReader<SeznamPouzivanehoNaradi>>();
            projection.Setup(p => p.Get("naradi-pouzivane")).Returns(view).Verifiable();
            var svc = new SeznamPouzivanehoNaradiService(projection.Object);
            
            var vysledky = svc.ZiskatSeznam().GetAwaiter().GetResult();

            projection.Verify();
            Assert.AreEqual(5, vysledky.Seznam.Count, "Pocet polozek");
            for (int i = 0; i < view.Seznam.Count; i++)
                Assert.AreEqual(view.Seznam[i], vysledky.Seznam[i], "Item {0}", i);
        }
    }
}
