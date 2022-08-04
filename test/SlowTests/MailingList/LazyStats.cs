using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class CanSearchLazily : RavenTestBase
    {
        public CanSearchLazily(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Take(15).ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    QueryStatistics stats;
                    var query = session.Query<User>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex_Embedded(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Take(15).ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    QueryStatistics stats;
                    var query = session.Query<User>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);
                }
            }
        }

        [Fact]
        public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstDynamicIndex_NonLazy()
        {
            using (var store = GetDocumentStore())
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Ayende"
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Take(15).ToList();
                    QueryStatistics stats;

                    var query = session.Query<User>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).ToList();

                    Assert.Equal(1, results.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.True(stats.TotalResults > 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstStaticIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Ayende"
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Query<User, UserByFirstName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Take(15).ToList();

                    QueryStatistics stats;

                    var query = session.Query<User, UserByFirstName>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    var enumerable = results.Value;//force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.True(stats.TotalResults > 0);
                }
            }
        }

        [Fact]
        public void CanGetTotalResultsFromStatisticsOnLazySearchAgainstStaticIndex_NonLazy()
        {
            using (var store = GetDocumentStore())
            {
                new UserByFirstName().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Ayende"
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Query<User, UserByFirstName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Take(15).ToList();

                    QueryStatistics stats;

                    var query = session.Query<User, UserByFirstName>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).ToList();

                    Assert.Equal(1, results.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.True(stats.TotalResults > 0);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class UserByFirstName : AbstractIndexCreationTask<User>
        {
            public UserByFirstName()
            {
                Map = users => from user in users
                               select new { user.FirstName };
            }
        }
    }
}
