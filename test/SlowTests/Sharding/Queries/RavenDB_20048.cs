using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Queries;

public class RavenDB_20048 : RavenTestBase
{
    public RavenDB_20048(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Map_Query_Default_Order_Should_Be_Consistent(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            FillData(store);

            using (var session = store.OpenSession())
            {
                var users = session.Query<User>()
                    .Where(x => x.Name != "Bob")
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i].Age);
                }
            }

            using (var session = store.OpenSession())
            {
                var users = session.Query<User>()
                    .Where(x => x.Name != "Bob")
                    .Select(x => x.Age)
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i]);
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Map_Query_Default_Order_Should_Be_Consistent_Static_Index(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var index = new Users_ByName();
            store.ExecuteIndex(index);

            FillData(store);

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var users = session.Query<User, Users_ByName>()
                    .Where(x => x.Name != "Bob")
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i].Age);
                }
            }

            using (var session = store.OpenSession())
            {
                var users = session.Query<User, Users_ByName>()
                    .Where(x => x.Name != "Bob")
                    .Select(x => x.Age)
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i]);
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Map_Query_Should_Default_Order_Should_Not_Throw_Missing_Last_Mosified_Static_Index_Stored(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var index = new Users_ByName_Stored();
            store.ExecuteIndex(index);

            FillData(store);

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var users = session.Query<User, Users_ByName_Stored>()
                    .Where(x => x.Name != "Bob")
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i].Age);
                }
            }

            using (var session = store.OpenSession())
            {
                // there is no order guarantee here, since all fields ('Age')
                // are extracted from index directly
                // so results do not have @last-modified
                var users = session.Query<User, Users_ByName_Stored>()
                    .Where(x => x.Name != "Bob")
                    .Select(x => x.Age)
                    .ToHashSet();

                Assert.Equal(20, users.Count);
                Assert.True(Enumerable.Range(0, 20).ToHashSet().SetEquals(users));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void Collection_Query_Default_Order_Should_Be_Consistent(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            FillData(store);

            using (var session = store.OpenSession())
            {
                var users = session.Query<User>()
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i].Age);
                }
            }

            using (var session = store.OpenSession())
            {
                var users = session.Query<User>()
                    .Select(x => x.Age)
                    .ToList();

                Assert.Equal(20, users.Count);

                for (var i = 0; i < 20; i++)
                {
                    Assert.Equal(i, users[i]);
                }
            }
        }
    }

    private static void FillData(IDocumentStore store)
    {
        for (var i = 0; i < 20; i++)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = $"User_{i}", Age = i });
                session.SaveChanges();
            }

            Thread.Sleep(1);
        }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users
                           select new
                           {
                               u.Name
                           };
        }
    }

    private class Users_ByName_Stored : AbstractIndexCreationTask<User>
    {
        public Users_ByName_Stored()
        {
            Map = users => from u in users
                select new
                {
                    u.Name,
                    u.Age
                };

            Store(x => x.Age, FieldStorage.Yes);
        }
    }
}
