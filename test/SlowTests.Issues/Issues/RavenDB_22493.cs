using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22493 : RavenTestBase
    {
        public RavenDB_22493(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanDeserializeDatabasesInfoWhenIndexingStatusIsNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "egor" });
                    await session.SaveChangesAsync();
                }

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));

                var client = store.GetRequestExecutor().HttpClient;
                var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/databases?name={store.Database}").WithConventions(store.Conventions));
                var result = await response.Content.ReadAsStringAsync();

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(result, "databases");
                    var databasesInfo = store.Conventions.Serialization.DefaultConverter.FromBlittable<DatabasesInfo>(bjro, "databases");

                    var databaseInfo = databasesInfo.Databases.Single(x => x.Name == store.Database);
                    Assert.Null(databaseInfo.IndexingStatus);
                }
            }
        }
    }
}
