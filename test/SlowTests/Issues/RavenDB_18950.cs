using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18950 : RavenTestBase
{
    public RavenDB_18950(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Generate_Proper_Identity_Server_Side(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            string id;

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "User 1" };
                await session.StoreAsync(user, "users/");

                await session.SaveChangesAsync();

                switch (options.DatabaseMode)
                {
                    case RavenDatabaseMode.Single:
                        Assert.Equal("users/0000000000000000001-A", user.Id);
                        id = user.Id;
                        break;
                    case RavenDatabaseMode.Sharded:
                        Assert.StartsWith("users/0000000000000000001-A$", user.Id);
                        id = user.Id;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id);
                Assert.NotNull(user);
            }
        }
    }
}
