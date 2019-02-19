using System;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetBugs : RavenTestBase
    {
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

        [Fact]
        public void CanUseStats()
        {
            using (var store = GetDocumentStore())
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
