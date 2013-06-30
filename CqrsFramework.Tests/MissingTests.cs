using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Tests
{
    [TestClass]
    public class MissingTests
    {
        [TestMethod]
        public void KeyValueProjectionAutoRegister()
        {
            Missing(typeof(CqrsFramework.Messaging.KeyValueProjectionAutoRegister<>));
        }

        [TestMethod]
        public void HashsetMessageDeduplicator()
        {
            Missing(typeof(CqrsFramework.ServiceBus.HashsetMessageDeduplicator));
        }

        private void Report<T>() { Missing(typeof(T)); }
        private void Missing(Type type) { Assert.Inconclusive("Type {0} has no tests", type.FullName); }
    }
}
