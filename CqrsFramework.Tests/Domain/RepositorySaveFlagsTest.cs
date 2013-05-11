using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Domain
{
    [TestClass]
    public class RepositorySaveFlagsTest
    {
        [TestMethod]
        public void OperationCreate()
        {
            var flags = RepositorySaveFlags.Create;
            Assert.IsTrue(flags.IsModeCreateNew());
            Assert.IsFalse(flags.IsModeAppend());
            Assert.IsFalse(flags.HasExpectedVersion());
            Assert.IsFalse(flags.HasSnapshotLimit());
        }

        [TestMethod]
        public void OperationAppend()
        {
            var flags = RepositorySaveFlags.Append;
            Assert.IsFalse(flags.IsModeCreateNew());
            Assert.IsTrue(flags.IsModeAppend());
            Assert.IsFalse(flags.HasExpectedVersion());
            Assert.IsFalse(flags.HasSnapshotLimit());
        }

        [TestMethod]
        public void ExpectedVersion()
        {
            var flags = RepositorySaveFlags.Append.ToVersion(5);
            Assert.IsFalse(flags.IsModeCreateNew());
            Assert.IsTrue(flags.IsModeAppend());
            Assert.IsTrue(flags.HasExpectedVersion());
            Assert.AreEqual(5, flags.ExpectedVersion());
            Assert.IsFalse(flags.HasSnapshotLimit());
        }

        [TestMethod]
        public void WithSnapshot()
        {
            var flags = RepositorySaveFlags.Create.WithSnapshot;
            Assert.IsTrue(flags.IsModeCreateNew());
            Assert.IsFalse(flags.IsModeAppend());
            Assert.IsFalse(flags.HasExpectedVersion());
            Assert.IsTrue(flags.HasSnapshotLimit());
            Assert.AreEqual(1, flags.SnapshotLimit());
        }

        [TestMethod]
        public void WithoutSnapshot()
        {
            var flags = RepositorySaveFlags.Append.WithoutSnapshot;
            Assert.IsFalse(flags.IsModeCreateNew());
            Assert.IsTrue(flags.IsModeAppend());
            Assert.IsFalse(flags.HasExpectedVersion());
            Assert.IsTrue(flags.HasSnapshotLimit());
            Assert.AreEqual(0, flags.SnapshotLimit());
        }

        [TestMethod]
        public void WithSnapshotLimit()
        {
            var flags = RepositorySaveFlags.Append.WithSnapshotFor(100);
            Assert.IsFalse(flags.IsModeCreateNew());
            Assert.IsTrue(flags.IsModeAppend());
            Assert.IsFalse(flags.HasExpectedVersion());
            Assert.IsTrue(flags.HasSnapshotLimit());
            Assert.AreEqual(100, flags.SnapshotLimit());
        }

    
    }
}
