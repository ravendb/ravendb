using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class LazyCount : RavenTest
    {
        [Fact]
        public void CanLazilyCountOnSearchAgainstDynamicIndex()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
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

                    RavenQueryStatistics stats;
                    var query = session.Query<User>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
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
            using (var store = NewDocumentStore())
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

                    RavenQueryStatistics stats;
                    var query = session.Query<User>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
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
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
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

                    RavenQueryStatistics stats;

                    var query = session.Query<User, UserByFirstName>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
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
            using (var store = NewDocumentStore())
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

                    RavenQueryStatistics stats;

                    var query = session.Query<User, UserByFirstName>().Statistics(out stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende");

                    var results = query.Take(8).Lazily();

                    // Want to be able to support these 2 scenarios for LazyCount!!
                    var lazyCount = query.CountLazily(); // filtered, only 1 match
                    var baseLazyCount = session.Query<User>().CountLazily(); // no filter, will match all Users

                    var enumerable = results.Value; //force evaluation
                    Assert.Equal(1, enumerable.Count());
                    Assert.Equal(DateTime.Now.Year, stats.IndexTimestamp.Year);
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
