using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Util;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class Any : RavenTestBase
    {
        private class TestDoc
        {
            public string SomeProperty { get; set; }
            public string[] StringArray { get; set; }
            public List<string> StringList { get; set; }
        }

        [Fact]
        public void CanQueryArrayWithAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { StringArray = new[] { "test", "doc", "foo" } };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var otherDoc = new TestDoc { SomeProperty = "foo" };
                    var doc = (from ar in session.Query<TestDoc>()
                               where ar.StringArray.Any(ac => ac == otherDoc.SomeProperty)
                               select ar).FirstOrDefault();
                    Assert.NotNull(doc);
                }
            }
        }

        [Fact]
        public void CanCountWithAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { StringArray = new[] { "one", "two" } });
                    session.Store(new TestDoc { StringArray = new string[0] });
                    session.Store(new TestDoc { StringArray = new string[0] });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any()));
                }
            }
        }

        [Fact]
        public void CanCountWithLengthGreaterThenZero()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Value", StringArray = new[] { "one", "two" } });
                    session.Store(new TestDoc { SomeProperty = "Value", StringArray = new string[0] });
                    session.Store(new TestDoc { SomeProperty = "Value", StringArray = new string[0] });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var count = session.Query<TestDoc>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(p => p.StringArray.Length > 0 && p.SomeProperty == "Value");
                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void CanCountWithCountGreaterThenZero()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Value", StringList = new List<string> { "one", "two" } });
                    session.Store(new TestDoc { SomeProperty = "Value", StringList = new List<string>() });
                    session.Store(new TestDoc { SomeProperty = "Value", StringList = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var count = session.Query<TestDoc>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(p => p.StringList.Count > 0 && p.SomeProperty == "Value");

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void EmptyArraysShouldBeCountedProperlyWhenUsingAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { StringArray = new[] { "one", "two" } });
                    session.Store(new TestDoc { StringArray = new string[0] });
                    session.Store(new TestDoc { StringArray = new string[0] });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any() == false));
                }
            }
        }

        [Fact]
        public void CanCountNullArraysWithAnyIfHaveAnotherPropertyStoredInTheIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Test", StringArray = new[] { "one", "two" } });
                    session.Store(new TestDoc { SomeProperty = "Test", StringArray = new string[0] });
                    session.Store(new TestDoc { SomeProperty = "Test", StringArray = new string[0] });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any() == false && p.SomeProperty == "Test"));
                }
            }
        }

        private class OrderableEntity
        {
            public DateTime Order { get; set; }
        }

        [Fact]
        public void NullRefWhenQuerying()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    DateTime dateTime = SystemTime.UtcNow;
                    var query = from a in session.Query<OrderableEntity>()
                                                    .Customize(x => x.WaitForNonStaleResults())
                                where dateTime < a.Order
                                select a;

                    query.ToList();

                }
            }
        }
    }
}
