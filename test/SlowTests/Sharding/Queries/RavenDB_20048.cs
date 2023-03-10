using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents;
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
    public void Map_Query_Should_Default_Order_Should_Be_Consistent(Options options)
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
    public void Collection_Query_Should_Default_Order_Should_Be_Consistent(Options options)
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

}
