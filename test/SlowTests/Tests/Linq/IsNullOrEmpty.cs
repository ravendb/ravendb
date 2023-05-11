using FastTests;
using Xunit;
using System.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class IsNullOrEmpty : RavenTestBase
    {
        public IsNullOrEmpty(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDoc
        {
            public string SomeProperty { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void IsNullOrEmptyEqTrue(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty)));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void IsNullOrEmptyEqFalse(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty) == false));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void IsNullOrEmptyNegated(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => !string.IsNullOrEmpty(p.SomeProperty)));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void WithAny(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => p.SomeProperty.Any()));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void WithAnyEqFalse(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Count(p => p.SomeProperty.Any() == false));
                }
            }
        }
    }
}
