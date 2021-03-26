using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10052 : RavenTestBase
    {
        public RavenDB_10052(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCompileIndex1()
        {
            using (var store = GetDocumentStore())
            {
                var index = new LastAccessPerUserDateTimeIndex();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    var now = RavenTestHelper.UtcToday;
                    session.Store(new User
                    {
                        Name = "Grisha",
                        LoginsByDate = new SortedDictionary<DateTime, LastAccess>
                        {
                            {
                                now.AddHours(-1), new LastAccess
                                {
                                    Count = 3
                                }
                            },
                            {
                                now.AddHours(-2), new LastAccess
                                {
                                    Count = 10
                                }
                            }
                        }
                    });

                    session.Store(new User
                    {
                        Name = "Karmel"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<User, LastAccessPerUserDateTimeIndex>()
                        .ProjectInto<LastAccessPerUserDateTimeIndex.Result>().ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal("Grisha", list[0].Name);
                    Assert.Equal(3, list[0].LastAccess.Count);
                }

                AssertIndexHasNoErrors(store, index.IndexName);
            }
        }

        [Fact]
        public void CanCompileIndex2()
        {
            using (var store = GetDocumentStore())
            {
                var index = new LastAccessPerUserDateTimeIndex();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    var now = RavenTestHelper.UtcToday;
                    session.Store(new User
                    {
                        Name = "Grisha",
                        LoginsByDate = new SortedDictionary<DateTime, LastAccess>
                        {
                            { now.AddHours(-1), new LastAccess
                            {
                                Count = 3
                            }},
                            { now.AddHours(-2), new LastAccess
                            {
                                Count = 10
                            }}
                        }
                    });

                    session.Store(new User
                    {
                        Name = "Karmel",
                        LoginsByDate = new SortedDictionary<DateTime, LastAccess>
                        {
                            { now.AddHours(-4), new LastAccess()}
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<User, LastAccessPerUserDateTimeIndex>()
                        .ProjectInto<LastAccessPerUserDateTimeIndex.Result>().ToList();
                    Assert.Equal(2, list.Count);
                    Assert.Equal("Grisha", list[0].Name);
                    Assert.Equal(3, list[0].LastAccess.Count);
                    Assert.Equal("Karmel", list[1].Name);
                    Assert.Equal(0, list[1].LastAccess.Count);
                }

                AssertIndexHasNoErrors(store, index.IndexName);
            }
        }

        [Fact]
        public void CanCompileIndex3()
        {
            using (var store = GetDocumentStore())
            {
                var index = new LastAccessPerUserDateTimeDefaultIndex();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    var now = RavenTestHelper.UtcToday;
                    session.Store(new User
                    {
                        Name = "Grisha",
                        LoginsByDate = new SortedDictionary<DateTime, LastAccess>
                        {
                            { now.AddHours(-1), new LastAccess
                            {
                                Count = 987
                            }},
                            { now.AddHours(-2), new LastAccess
                            {
                                Count = 32
                            }}
                        }
                    });

                    session.Store(new User
                    {
                        Name = "Karmel"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<User, LastAccessPerUserDateTimeDefaultIndex>()
                        .ProjectInto<LastAccessPerUserDateTimeDefaultIndex.Result>().ToList();
                    Assert.Equal(2, list.Count);
                    Assert.Equal("Grisha", list[0].Name);
                    Assert.Equal(987, list[0].LastAccess.Count);
                    Assert.Equal("Karmel", list[1].Name);
                    Assert.Equal(0, list[1].LastAccess.Count);
                }

                AssertIndexHasNoErrors(store, index.IndexName);
            }
        }

        [Fact]
        public void CanCompileIndex4()
        {
            using (var store = GetDocumentStore())
            {
                var index = new LastAccessPerUserTicksIndex();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    var now = RavenTestHelper.UtcToday;
                    session.Store(new User
                    {
                        Name = "Grisha"
                    });

                    session.Store(new User
                    {
                        Name = "Karmel",
                        LoginsByTicks = new SortedDictionary<DateTime, long>
                        {
                            { now.AddHours(-1), 12321232},
                            { now.AddHours(-2), 98172832}
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<User, LastAccessPerUserTicksIndex>()
                        .ProjectInto<LastAccessPerUserTicksIndex.Result>().ToList();
                    Assert.Equal(1, list.Count);
                    Assert.Equal("Karmel", list[0].Name);
                    Assert.Equal(12321232, list[0].LastAccessTicks);
                }

                AssertIndexHasNoErrors(store, index.IndexName);
            }
        }

        private static void AssertIndexHasNoErrors(IDocumentStore store, string indexName)
        {
            Assert.Equal(0, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName }))[0].Errors.Length);
        }

        private class User
        {
            public string Name { get; set; }

            public SortedDictionary<DateTime, LastAccess> LoginsByDate { get; set; }

            public SortedDictionary<DateTime, long> LoginsByTicks { get; set; }
        }

        private class LastAccess
        {
            public long Count { get; set; }
        }

        private class LastAccessPerUserDateTimeIndex : AbstractIndexCreationTask<User, LastAccessPerUserDateTimeIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public LastAccess LastAccess { get; set; }
            }

            public LastAccessPerUserDateTimeIndex()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        LastAccess = user.LoginsByDate.Last().Value
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class LastAccessPerUserDateTimeDefaultIndex : AbstractIndexCreationTask<User, LastAccessPerUserDateTimeIndex.Result>
        {
            private readonly LastAccess _defaultLastAccess = new LastAccess();

            public class Result
            {
                public string Name { get; set; }

                public LastAccess LastAccess { get; set; }
            }

            public LastAccessPerUserDateTimeDefaultIndex()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        LastAccess = user.LoginsByDate.Count > 0 ? user.LoginsByDate.Last().Value : new LastAccess
                        {
                            Count = 0
                        }
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class LastAccessPerUserTicksIndex : AbstractIndexCreationTask<User, LastAccessPerUserTicksIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }

                public long LastAccessTicks { get; set; }
            }

            public LastAccessPerUserTicksIndex()
            {
                Map = users =>
                    from user in users
                    select new Result
                    {
                        LastAccessTicks = user.LoginsByTicks.Last().Value
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
