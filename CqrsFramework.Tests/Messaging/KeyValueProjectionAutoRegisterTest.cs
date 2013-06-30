using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqrsFramework.Messaging;
using System.Xml.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqrsFramework.Tests.Messaging
{
    [TestClass]
    public class KeyValueProjectionAutoRegisterTest
    {
        private class TestView
        {
            public StringBuilder Str = new StringBuilder();
        }

        private class TestProjection
        {
            public string GetKey(XElement elem)
            {
                return elem.Name.LocalName;
            }

            public string GetKey(XAttribute att)
            {
                return att.Name.LocalName;
            }

            public string GetKey(string s)
            {
                return s;
            }

            public string GetKey(int i)
            {
                return i.ToString();
            }

            public TestView Handle(XElement elem, MessageHeaders headers)
            {
                var view = new TestView();
                view.Str.Append(elem.ToString());
                return view;
            }

            public TestView Handle(XElement elem, MessageHeaders headers, TestView view)
            {
                view.Str.Append(elem.ToString());
                return view;
            }

            public TestView When(XAttribute att)
            {
                var view = new TestView();
                view.Str.Append(att.Value);
                return view;
            }

            public TestView When(XAttribute att, TestView view)
            {
                view.Str.Append(att.Value);
                return view;
            }

            public void When(string s, MessageHeaders headers, TestView view)
            {
                view.Str.Append(s);
            }

            public void When(int i, TestView view)
            {
                view.Str.Append(i);
            }
        }

        private MessageHeaders BuildHeaders()
        {
            return new MessageHeaders();
        }

        [TestMethod]
        public void DiscoverProjectionMethods()
        {
            var projection = new TestProjection();
            var reg = new KeyValueProjectionAutoRegister<TestView>(projection);
            var methods = reg.FindMethods().ToList();
            Assert.AreEqual(4, methods.Count);
            foreach (var m in methods)
            {
                switch (m.Type.Name)
                {
                    case "XElement":
                        Assert.AreEqual("Hello", reg.MakeGetKey(m)(new XElement("Hello")));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeAdd(m)(new XElement("Hello"), BuildHeaders()).Str.ToString()));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeUpdate(m)(new XElement("Hello"), BuildHeaders(), new TestView()).Str.ToString()));
                        break;
                    case "XAttribute":
                        Assert.AreEqual("Att", reg.MakeGetKey(m)(new XAttribute("Att", "Value")));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeAdd(m)(new XAttribute("Att", "Value"), BuildHeaders()).Str.ToString()));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeUpdate(m)(new XAttribute("Att", "Value"), BuildHeaders(), new TestView()).Str.ToString()));
                        break;
                    case "String":
                        Assert.AreEqual("Hello", reg.MakeGetKey(m)("Hello"));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeAdd(m)("Hello", BuildHeaders()).Str.ToString()));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeUpdate(m)("Hello", BuildHeaders(), new TestView()).Str.ToString()));
                        break;
                    case "Int32":
                        Assert.AreEqual("584", reg.MakeGetKey(m)(584));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeAdd(m)(5842, BuildHeaders()).Str.ToString()));
                        Assert.IsFalse(string.IsNullOrEmpty(reg.MakeUpdate(m)(5842, BuildHeaders(), new TestView()).Str.ToString()));
                        break;
                }

            }
        }
    }
}
