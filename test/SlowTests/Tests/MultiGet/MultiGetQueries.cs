using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetQueries : RavenTestBase
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
        public void UnlessAccessedLazyQueriesAreNoOp()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void WithPaging()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Age == 0).Skip(1).Take(2).Lazily();
                    Assert.Equal(2, result1.Value.ToArray().Length);
                }
            }
        }


        [Fact]
        public void CanGetQueryStats()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User { Age = 3 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats1;
                    var result1 = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats1)
                        .Where(x => x.Age == 0).Skip(1).Take(2)
                        .Lazily();

                    QueryStatistics stats2;
                    var result2 = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats2)
                        .Where(x => x.Age == 3).Skip(1).Take(2)
                        .Lazily();
                    Assert.Equal(2, result1.Value.ToArray().Length);
                    Assert.Equal(3, stats1.TotalResults);

                    Assert.Equal(0, result2.Value.ToArray().Length);
                    Assert.Equal(1, stats2.TotalResults);
                }

            }
        }

        [Fact]
        public void WithQueuedActions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    IEnumerable<User> users = null;
                    session.Query<User>().Where(x => x.Age == 0).Skip(1).Take(2).Lazily(x => users = x);
                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.Equal(2, users.Count());
                }

            }
        }

        [Fact]
        public void WithQueuedActions_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    User user = null;
                    session.Advanced.Lazily.Load<User>("users/1-A", x => user = x);
                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(user);
                }

            }
        }

        [Fact]
        public void LazyOperationsAreBatched()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
                    Assert.Empty(result2.Value);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Empty(result1.Value);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

            }
        }

        [Fact]
        public void LazyMultiLoadOperationWouldBeInTheSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "oren")
                        .ToList();
                }
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
                    Assert.NotEmpty(result2.Value);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotEmpty(result1.Value);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }

            }
        }

        [Fact]
        public void LazyWithProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User { Name = "ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "oren")
                        .ToList();
                }
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren")
                        .Select(x => new { x.Name })
                        .Lazily();

                    Assert.Equal("oren", result1.Value.First().Name);
                }

            }
        }


        [Fact]
        public void LazyWithProjection2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User { Name = "ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "oren")
                        .ToList();
                }
                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Where(x => x.Name == "oren")
                        .Select(x => new { x.Name })
                        .ToArray();

                    Assert.Equal("oren", result1.First().Name);
                }

            }
        }

        [Fact]
        public void LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>().ToArray();

                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(5))).Where(x => x.Name == "ayende").Lazily();
                    Assert.NotEmpty(result2.Value);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotEmpty(result1.Value);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }

            }
        }

        [Fact]
        public void CanGetStatisticsWithLazyQueryResults()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "test")
                        .ToList();
                }
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    QueryStatistics stats2;
                    var result1 = session.Query<User>().Statistics(out stats).Where(x => x.Name == "oren").Lazily();
                    var result2 = session.Query<User>().Statistics(out stats2).Where(x => x.Name == "ayende").Lazily();
                    Assert.NotEmpty(result2.Value);

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(1, stats2.TotalResults);
                }

            }
        }
    }
}
