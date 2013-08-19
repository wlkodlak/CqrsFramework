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

namespace Vydejna.Tests.PouzivaneNaradiWriteServiceTests
{
    [TestClass]
    public class DefinovatNaradiTest : NaradiTestBase
    {
        [TestMethod]
        public void UspesneDefinovatNaradi()
        {
            var cmd = new DefinovatPouzivaneNaradiCommand()
            {
                Id = new Guid("C3DD077F-3260-4780-B477-F9282540204E"),
                Vykres = "vykres-01",
                Rozmer = "rozmer-01.a",
                Druh = "A-kategorie"
            };
            PripravitService(null);
            Service.DefinovatPouzivaneNaradi(cmd);
            Assert.AreEqual(1, UlozeneUdalosti.Count, "Pocet eventu");
            Assert.IsInstanceOfType(UlozeneUdalosti[0], typeof(DefinovanoPouzivaneNaradiEvent), "Typ eventu");
            var event0 = (DefinovanoPouzivaneNaradiEvent)UlozeneUdalosti[0];
            Assert.AreEqual(cmd.Id, event0.Id, "event.Id");
            Assert.AreEqual(cmd.Vykres, event0.Vykres, "event.Vykres");
            Assert.AreEqual(cmd.Rozmer, event0.Rozmer, "event.Rozmer");
            Assert.AreEqual(cmd.Druh, event0.Druh, "event.Druh");
        }

        [TestMethod]
        public void ChybaValidacePrikazuDefiniceNaradi()
        {
            var cmd = new DefinovatPouzivaneNaradiCommand()
            {
                Vykres = "",
                Rozmer = "rozmer-01.a",
                Druh = ""
            };
            try
            {
                PripravitService(null);
                Service.DefinovatPouzivaneNaradi(cmd);
                Assert.Fail("Ocekavana vyjimka DomainErrorException");
            }
            catch (DomainErrorException)
            {
                Assert.AreEqual(0, UlozeneUdalosti.Count);
            }
        }



    }
}
