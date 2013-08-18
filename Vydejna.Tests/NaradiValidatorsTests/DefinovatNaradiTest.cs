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
    }
}
