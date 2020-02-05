using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11604 : RavenTestBase
    {
        public RavenDB_11604(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public int Group { get; set; }
        }

        private class User2
        {
            public Group Group { get; set; }
        }

        private class Group
        {
            public string Order { get; set; }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Field_To_Fetch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = new User
                        {
                            FirstName = "Test",
                            LastName = "User",
                            Group = i % 4
                        };
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(user => new
                        {
                            user.Group
                        });

                    Assert.Equal("from 'Users' as __alias0 " +
                                 "select __alias0.'Group'"
                                , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(10, results.Count);
                    foreach (var r in results)
                    {
                        Assert.InRange(r.Group, 0, 3);
                    }
                }
            }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Field_To_Fetch_With_Other_Fields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = new User
                        {
                            FirstName = "Test",
                            LastName = "User",
                            Group = i % 4
                        };
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(user => new
                        {
                            user.FirstName,
                            user.Group,
                            user.LastName
                        });

                    Assert.Equal("from 'Users' as __alias0 " +
                                 "select __alias0.FirstName, " +
                                    "__alias0.'Group', " +
                                    "__alias0.LastName"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(10, results.Count);
                    foreach (var r in results)
                    {
                        Assert.Equal("Test", r.FirstName);
                        Assert.Equal("User", r.LastName);

                        Assert.InRange(r.Group, 0, 3);
                    }
                }
            }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Field_To_Fetch_And_Select_Counter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = new User
                        {
                            FirstName = "Test",
                            Group = i % 4
                        };
                        session.Store(user);
                        session.CountersFor(user).Increment("likes", 100 * i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(user => new
                        {
                            user.FirstName,
                            user.Group,
                            Likes = RavenQuery.Counter(user, "likes")
                        });

                    Assert.Equal("from 'Users' as __alias0 " +
                                 "select __alias0.FirstName, " +
                                 "__alias0.'Group', " +
                                 "counter(__alias0, likes) as Likes"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(10, results.Count);
                    for (var index = 0; index < results.Count; index++)
                    {
                        Assert.Equal("Test", results[index].FirstName);
                        Assert.InRange(results[index].Group, 0, 3);
                        Assert.Equal(index * 100, results[index].Likes);
                    }
                }
            }
        }

        [Fact]
        public void Can_Use_RQL_Reserved_Words_As_Field_To_Fetch_With_Nested_Path()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = new User2
                        {
                            Group = new Group
                            {
                                Order = "orders/" + i
                            }
                        };
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User2>()
                        .Select(user => new
                        {
                            user.Group.Order
                        });

                    Assert.Equal("from 'User2s' as __alias0 " +
                                 "select __alias0.'Group'.Order as 'Order'"
                                , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(10, results.Count);

                    for (var i = 0; i < results.Count; i++)
                    {
                        Assert.Equal(results[i].Order, "orders/" + i);
                    }
                }
            }
        }

    }
}
