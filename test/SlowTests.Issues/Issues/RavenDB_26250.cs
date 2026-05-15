using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26250 : RavenTestBase
    {
        public RavenDB_26250(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string FirstName { get; set; }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void Statistics_ShouldBePopulated_WhenCalledAfterToDocumentQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Person { FirstName = "Anne" });
                session.Store(new Person { FirstName = "Robert" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var results = session.Query<Person>()
                    .Where(x => x.FirstName == "Anne")
                    .ToDocumentQuery()
                    .WaitForNonStaleResults()
                    .Statistics(out QueryStatistics stats)
                    .ToList();

                Assert.NotNull(stats.IndexName);
                Assert.Equal(1, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void Statistics_ShouldBePopulated_WhenCalledOnBothSidesOfToDocumentQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Person { FirstName = "Anne" });
                session.Store(new Person { FirstName = "Robert" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var results = session.Query<Person>()
                    .Statistics(out QueryStatistics stats5)
                    .ToDocumentQuery()
                    .WaitForNonStaleResults()
                    .WhereEquals(x => x.FirstName, "Anne")
                    .OrElse()
                    .WhereEquals(x => x.FirstName, "Robert")
                    .Statistics(out QueryStatistics stats6)
                    .ToList();

                Assert.NotNull(stats5.IndexName);
                Assert.NotNull(stats6.IndexName);
                Assert.Equal(2, results.Count);
            }
        }
    }
}
