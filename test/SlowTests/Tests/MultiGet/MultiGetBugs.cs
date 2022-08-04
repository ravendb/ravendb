using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetBugs : RavenTestBase
    {
        public MultiGetBugs(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Info { get; set; }
            public bool Active { get; set; }
            public DateTime Created { get; set; }

            public User()
            {
                Name = string.Empty;
                Age = default(int);
                Info = string.Empty;
                Active = false;
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseStats(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Lazily();

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    Assert.Equal(2, stats.TotalResults);
                }
            }
        }
    }
}
