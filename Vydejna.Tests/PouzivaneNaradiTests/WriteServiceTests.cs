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
    }
}
