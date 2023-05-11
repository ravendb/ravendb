using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class Any : RavenTestBase
    {
        public Any(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDoc
        {
            public string SomeProperty { get; set; }
            public string[] StringArray { get; set; }
            public List<string> StringList { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryArrayWithAny(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCountWithAny(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCountWithLengthGreaterThenZero(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCountWithCountGreaterThenZero(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void EmptyArraysShouldBeCountedProperlyWhenUsingAny(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCountNullArraysWithAnyIfHaveAnotherPropertyStoredInTheIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void NullRefWhenQuerying(Options options)
        {
            using (var store = GetDocumentStore(options))
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
