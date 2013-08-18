using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Moq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Vydejna.Contracts;
using CqrsFramework.Domain;

namespace Vydejna.Tests.NaradiValidatorsTests
{
    [TestClass]
    public class DefinovatNaradiTest
    {
        [TestMethod]
        public void VDefiniciChybiVykres()
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

        [TestMethod]
        public void TestDuplicityVykresuARozmeru()
        {
            var cmd = new DefinovatPouzivaneNaradiCommand()
            {
                Vykres = "vykres1",
                Rozmer = "rozmer-1",
                Druh = ""
            };
            var dupsvc = new Mock<IPrehledNaradiReadService>();
            dupsvc.Setup(s => s.ExistujeVykresARozmer("vykres1", "rozmer-1")).Returns(true).Verifiable();
            var validator = new DefinovatPouzivaneNaradiCommandValidator(dupsvc.Object);
            var result = validator.Validate(cmd);
            Assert.AreEqual(ValidationRuleSeverity.Error, result.Severity);
            Assert.AreEqual("CONFLICT:Vykres+Rozmer", result.BrokenRules[0].ErrorCode);
        }
    }
}
