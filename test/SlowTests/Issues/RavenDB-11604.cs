using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11604 : RavenTestBase
    {
        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public int Group { get; set; }
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

                    Assert.Equal("from Users as __ravenDefaultAlias0 " +
                                 "select __ravenDefaultAlias0.'Group'"
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

                    Assert.Equal("from Users as __ravenDefaultAlias0 " +
                                 "select __ravenDefaultAlias0.FirstName, " +
                                    "__ravenDefaultAlias0.'Group', " +
                                    "__ravenDefaultAlias0.LastName"
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

    }
}
