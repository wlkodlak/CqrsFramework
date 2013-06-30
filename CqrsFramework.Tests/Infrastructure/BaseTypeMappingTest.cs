using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Infrastructure;
using System.Collections;
using System.Data;

namespace CqrsFramework.Tests.Infrastructure
{
    [TestClass]
    public class BaseTypeMappingTest
    {
        private BaseTypeMapping<string> _map;

        [TestInitialize]
        public void Initialize()
        {
            _map = new BaseTypeMapping<string>();
        }

        private void Add<T>()
        {
            _map.Add(typeof(T), typeof(T).Name);
        }

        private void AddThrows<T>()
        {
            try
            {
                Add<T>();
                Assert.Fail("Expected ArgumentException");
            }
            catch (ArgumentException)
            {
            }
        }

        private void VerifyExact<T>()
        {
            Assert.AreEqual(typeof(T).Name, _map.Get(typeof(T)));
        }

        private void Verify<T>(Type expected)
        {
            if (expected == null)
                Assert.IsNull(_map.Get(typeof(T)), typeof(T).Name);
            else
                Assert.AreEqual(expected.Name, _map.Get(typeof(T)));
        }

        private void VerifyAll<T>(params Type[] expected)
        {
            var results = _map.GetAll(typeof(T)).OrderBy(s => s).ToList();
            var names = expected.Select(t => t.Name).OrderBy(s => s).ToList();
            AssertExtension.AreEqual(names, results);
        }

        private void VerifyException<T>()
        {
            try
            {
                _map.Get(typeof(T));
                Assert.Fail("Expected InvalidOperationException for {0}", typeof(T).Name);
            }
            catch (InvalidOperationException)
            {
            }
        }

        [TestMethod]
        public void ConcreteTypesOnly()
        {
            Add<ArgumentOutOfRangeException>();
            Add<ArgumentNullException>();
            Add<InvalidCastException>();
            VerifyExact<ArgumentOutOfRangeException>();
            VerifyExact<ArgumentNullException>();
            VerifyExact<InvalidCastException>();
            Verify<FormatException>(null);
        }

        [TestMethod]
        public void GetRegisteredBase()
        {
            Add<ArgumentException>();
            Add<FormatException>();
            Add<ArithmeticException>();
            Verify<ArgumentNullException>(typeof(ArgumentException));
            Verify<UriFormatException>(typeof(FormatException));
            Verify<DivideByZeroException>(typeof(ArithmeticException));
        }

        [TestMethod]
        public void GetAllBaseTypes()
        {
            Add<ArgumentException>();
            Add<FormatException>();
            Add<ArithmeticException>();
            Add<SystemException>();
            Add<Exception>();
            Add<DivideByZeroException>();
            VerifyAll<DivideByZeroException>(typeof(DivideByZeroException), typeof(ArithmeticException), typeof(SystemException), typeof(Exception));
            VerifyAll<FormatException>(typeof(FormatException), typeof(SystemException), typeof(Exception));
        }

        [TestMethod]
        public void AllWithInterfaces()
        {
            Add<ICloneable>();
            Add<IEnumerable>();
            Add<ISupportInitialize>();
            Add<IConvertible>();
            VerifyAll<DataTable>(typeof(ISupportInitialize));
            VerifyAll<string>(typeof(ICloneable), typeof(IEnumerable), typeof(IConvertible));
            VerifyAll<ArrayList>(typeof(ICloneable), typeof(IEnumerable));
        }

        [TestMethod]
        public void SingleModes()
        {
            Add<ICloneable>();
            Add<IEnumerable>();
            Add<ISupportInitialize>();
            Add<IConvertible>();
            Verify<DataTable>(typeof(ISupportInitialize));
            VerifyException<string>();
        }

        [TestMethod]
        public void AddingAfterUsing()
        {
            Add<IEnumerable>();
            Add<ISupportInitialize>();
            Add<IConvertible>();
            VerifyAll<DataTable>(typeof(ISupportInitialize));
            VerifyAll<string>(typeof(IEnumerable), typeof(IConvertible));
            VerifyAll<ArrayList>(typeof(IEnumerable));
            Add<ICloneable>();
            VerifyAll<string>(typeof(ICloneable), typeof(IEnumerable), typeof(IConvertible));
            VerifyAll<ArrayList>(typeof(ICloneable), typeof(IEnumerable));
        }

        [TestMethod]
        public void DuplicationsDisabled()
        {
            Add<IEnumerable>();
            Add<ISupportInitialize>();
            AddThrows<IEnumerable>();
        }
    }
}
