using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Gui.SeznamNaradi;
using Vydejna.Contracts;
using Moq;
using System.Collections.Specialized;

namespace Vydejna.Tests.SeznamPouzivanehoNaradiTests
{
    [TestClass]
    public class SeznamNaradiViewModelTest
    {
        [TestMethod]
        public void NacitaDataPriKonstrukci()
        {
            var vysledky = new SeznamPouzivanehoNaradi();
            vysledky.Seznam = new List<PolozkaSeznamuPouzivanehoNaradi>()
            {
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="5847-5877", Rozmer="80x40x20", Druh="Druh naradi"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="2144-5345", Rozmer="120x50x20", Druh="Klestina"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="3481-7484", Rozmer="800x20", Druh="Brusny papir"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="22-254-51", Rozmer="175x35x5", Druh="Datelna"},
                new PolozkaSeznamuPouzivanehoNaradi { Id=Guid.Empty, Vykres="2487-5448", Rozmer="defektni", Druh="Spacek"}
            };
            var vysledkyTask = new TaskCompletionSource<SeznamPouzivanehoNaradi>();
            var svc = new Mock<ISeznamPouzivanehoNaradiService>();
            svc.Setup(s => s.ZiskatSeznam()).Returns(vysledkyTask.Task);
            var vm = new SeznamNaradiViewModel(svc.Object);
            Assert.IsInstanceOfType(vm.SeznamNaradi, typeof(INotifyCollectionChanged));
            var changes = new List<NotifyCollectionChangedEventArgs>();
            (vm.SeznamNaradi as INotifyCollectionChanged).CollectionChanged += (s, e) => changes.Add(e);
            var nacitaniTask = vm.NacistData();
            vysledkyTask.SetResult(vysledky);
            nacitaniTask.GetAwaiter().GetResult();
            Assert.AreEqual(6, changes.Count, "Changes count");
            Assert.AreEqual(5, vm.SeznamNaradi.Count(), "List length");
            Assert.AreEqual("3481-7484", vm.SeznamNaradi.ToList()[2].Vykres, "[2].Vykres");
        }
    }
}
