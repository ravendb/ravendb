using FastTests;
using Xunit;
using System.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class CanQueryWithSavedKeywords : RavenTestBase
    {
        public CanQueryWithSavedKeywords(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDoc
        {
            public string SomeProperty { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryWithNot(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "NOT" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "NOT"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryWithOr(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "OR" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .FirstOrDefault(doc => doc.SomeProperty == "OR"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryWithAnd(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "AND" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "AND"));
                }
            }
        }
    }
}
