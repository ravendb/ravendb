using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class LazyCount : RavenTestBase
    {
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

        [Fact]
        public void CanLazilyCountOnSearchAgainstDynamicIndex()
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
                    session.Store(new User
                    {
                        FirstName = "SomethingElse"
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

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);

                    Assert.Equal(1, lazyCount.Value);
                    Assert.Equal(2, baseLazyCount.Value);

                    // All the lazy queries only caused 1 request
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanLazilyCountOnSearchAgainstDynamicIndex_Embedded()
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
                    session.Store(new User
                    {
                        FirstName = "SomethingElse"
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

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);

                    Assert.Equal(1, lazyCount.Value);
                    Assert.Equal(2, baseLazyCount.Value);

                    // All the lazy queries only caused 1 request
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanLazilyCountOnSearchAgainstStaticIndex()
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
                    session.Store(new User
                    {
                        FirstName = "SomethingElse"
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

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);

                    Assert.Equal(1, lazyCount.Value);
                    Assert.Equal(2, baseLazyCount.Value);

                    // All the lazy queries only caused 1 request
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanLazilyCountOnSearchAgainstStaticIndex_Embedded()
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
                    session.Store(new User
                    {
                        FirstName = "SomethingElse"
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

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.UtcNow.Year, stats.IndexTimestamp.Year);
                    Assert.Equal(1, stats.TotalResults);

                    Assert.Equal(1, lazyCount.Value);
                    Assert.Equal(2, baseLazyCount.Value);

                    // All the lazy queries only caused 1 request
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
