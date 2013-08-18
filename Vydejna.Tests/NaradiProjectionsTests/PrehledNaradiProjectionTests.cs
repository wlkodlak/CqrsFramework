using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vydejna.Contracts;
using Vydejna.Domain;
using Moq;
using CqrsFramework.KeyValueStore;
using CqrsFramework.Domain;
using CqrsFramework.Messaging;
using CqrsFramework.Serialization;

namespace Vydejna.Tests.NaradiProjectionsTests
{
    [TestClass]
    public class PrehledNaradiProjectionTests
    {
        private PrehledNaradiProjection _service;

        private void PripravitService(IEnumerable<IEvent> events)
        {
            var knownTypes = new List<Type>();
            knownTypes.Add(typeof(PrehledNaradiView));
            var resolver = new MessageTypeResolver();
            foreach (var type in knownTypes)
                resolver.RegisterType(type, type.FullName);
            var svc = new PrehledNaradiProjection(new MemoryKeyValueStore(), new JsonMessageBodySerializer(knownTypes, resolver));
            svc.Reset();
            svc.BeginUpdate();
            long clock = 1;
            foreach (var ev in events)
            {
                var msg = new Message(ev);
                msg.Headers.MessageId = Guid.NewGuid();
                msg.Headers.EventClock = clock;
                clock++;
                svc.Dispatch(msg);
            }
            _service = svc;
        }

        [TestMethod]
        public void ZiskatPrazdnySeznamPouzivanehoNaradi()
        {
            PripravitService(new IEvent[0]);
            var seznam = _service.ZiskatSeznam(0, int.MaxValue);
            Assert.AreEqual(0, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(0, seznam.SeznamNaradi.Count, "Pocet v kolekci");
        }

        private List<IEvent> UdalostiDefinicNaradi()
        {
            return new List<IEvent>()
                {
                    new DefinovanoPouzivaneNaradiEvent
                    {
                        Vykres = "vykres-1",
                        Rozmer = "rozmer-1a",
                        Druh = "kategorie01",
                        Id = new Guid("0B9EC0EC-B017-4FF5-8C69-DE9DBB66820A")
                    },
                    new DefinovanoPouzivaneNaradiEvent
                    {
                        Vykres = "vykres-2",
                        Rozmer = "rozmer-2a",
                        Druh = "kategorie01",
                        Id = new Guid("319CF496-8CEA-4133-BB21-D3DD9A333060")
                    },
                    new DefinovanoPouzivaneNaradiEvent
                    {
                        Vykres = "vykres-2",
                        Rozmer = "rozmer-2b",
                        Druh = "kategorie01",
                        Id = new Guid("CAC7FF44-E669-4B81-B9F9-79DD830C0250")
                    },
                    new DefinovanoPouzivaneNaradiEvent
                    {
                        Vykres = "vykres-3",
                        Rozmer = "rozmer-3c",
                        Druh = "kategorie06",
                        Id = new Guid("B65C9239-DEE8-4E05-8650-EF95160DE36F")
                    },
                    new DefinovanoPouzivaneNaradiEvent
                    {
                        Vykres = "vykres-4",
                        Rozmer = "rozmer-4a",
                        Druh = "kategorie00",
                        Id = new Guid("EAFFD9B6-E30A-44EF-A762-73082ADFD1D2")
                    },
                };
        }

        [TestMethod]
        public void ZiskatNaplnenySeznamVsehoPouzivanehoNaradi()
        {
            PripravitService(UdalostiDefinicNaradi());
            var seznam = _service.ZiskatSeznam(0, int.MaxValue);
            Assert.AreEqual(5, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(5, seznam.SeznamNaradi.Count, "Pocet v kolekci");
        }

        [TestMethod]
        public void ZiskatCastNaplnenehoSeznamuVsehoNaradi()
        {
            var events = UdalostiDefinicNaradi();
            PripravitService(events);
            var seznam = _service.ZiskatSeznam(1, 2);
            Assert.AreEqual(5, seznam.PocetVsechPrvku, "Pocet vsech prvku");
            Assert.AreEqual(2, seznam.SeznamNaradi.Count, "Pocet v kolekci");
            Assert.AreEqual(1, seznam.OffsetPrvnihoPrvku, "Offset");
            Assert.AreEqual((events[1] as DefinovanoPouzivaneNaradiEvent).Id, seznam.SeznamNaradi[0].Id, "Id[0]");
            Assert.AreEqual((events[2] as DefinovanoPouzivaneNaradiEvent).Id, seznam.SeznamNaradi[1].Id, "Id[1]");
        }

        [TestMethod]
        public void ExistenceVykresuARozmeru()
        {
            var events = UdalostiDefinicNaradi();
            PripravitService(events);
            Assert.IsTrue(_service.ExistujeVykresARozmer("vykres-4", "rozmer-4a"));
            Assert.IsFalse(_service.ExistujeVykresARozmer("vykres-4", "rozmer-3c"));
            Assert.IsFalse(_service.ExistujeVykresARozmer("vykres-0", "zakladni"));
        }
    }
}
