using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqrsFramework.Tests
{
    public class AssertExtension
    {
        public static void AreEqual(Message expected, Message actual, string message = "")
        {
            var comparer = new CompareObjects();
            comparer.MaxDifferences = 3;

            var expectedHeaders = expected.Headers.ToList();
            var actualHeaders = actual.Headers.ToList();
            if (!comparer.Compare(expectedHeaders, actualHeaders))
                throw new Exception(string.Concat(message, " in header ", comparer.DifferencesString));

            if (!comparer.Compare(expected.Payload, actual.Payload))
                throw new Exception(string.Concat(message, " in body ", comparer.DifferencesString));
        }

        public static void AreEqualStrings(byte[] expected, byte[] actual, string message = "")
        {
            var utf8 = new UTF8Encoding(false);
            var expectedString = utf8.GetString(expected);
            var actualString = utf8.GetString(actual);
            Assert.AreEqual(expectedString, actualString, message);
        }

        public static void AreEqual(byte[] expected, byte[] actual, string message = "")
        {
            var min = Math.Min(expected.Length, actual.Length);
            for (int i = 0; i < min; i++)
                Assert.AreEqual(expected[i], actual[i], "{1} Difference at {0}", i, message);
            Assert.AreEqual(expected.Length, actual.Length, "{0} Different lengths", message);
        }

        public static void AreEqual<T>(T expected, T actual, string message = "")
        {
            var comparer = new CompareObjects();
            comparer.MaxDifferences = 3;
            if (!comparer.Compare(expected, actual))
                throw new Exception(string.Concat(message, comparer.DifferencesString));
        }
    }
}
