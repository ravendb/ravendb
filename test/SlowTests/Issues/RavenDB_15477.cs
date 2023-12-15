using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15477 : RavenTestBase
    {
        public RavenDB_15477(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanDeserializeDatabaseInfoForDisabledDb()
        {
            using (var store = GetDocumentStore())
            {
                await new UserIndex().ExecuteAsync(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User() { Name = "egor" });
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(store);
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

                var client = store.GetRequestExecutor().HttpClient;
                var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/databases?name={store.Database}").WithConventions(store.Conventions));
                var result = await response.Content.ReadAsStringAsync();

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(result, "test");
                    Assert.True(bjro.TryGet(nameof(DatabasesInfo.Databases), out BlittableJsonReaderArray array));
                    Assert.Equal(1, array.Length);

                    var item = (BlittableJsonReaderObject)array[0];

                    Assert.True(item.TryGetMember(nameof(DatabaseInfo.IndexingStatus), out var indexingStatus));
                    Assert.Null(indexingStatus);
                    var dbInfo = JsonDeserializationServer.DatabaseInfo(item);
                    Assert.Equal(IndexRunningStatus.Running, dbInfo.IndexingStatus);
                }
            }
        }

        private class UserIndex : AbstractIndexCreationTask<User, UserIndex.Result>
        {
            public UserIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };

            }

            public class Result
            {
                public string Name { get; set; }
            }
        }
    }
}
