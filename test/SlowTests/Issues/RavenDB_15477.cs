using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Json;
using Sparrow.Json;
using Tests.Infrastructure;
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
        public void CanDeserializeDatabaseInfoForDisabledDb()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

                var client = store.GetRequestExecutor().HttpClient;
                var response = client.GetAsync(store.Urls.First() + $"/databases?name={store.Database}", CancellationToken.None).Result;
                var result = response.Content.ReadAsStringAsync().Result;

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.ReadForMemory(result, "test");
                    Assert.True(bjro.TryGetMember(nameof(DatabaseInfo.IndexingStatus), out var indexingStatus));
                    Assert.Null(indexingStatus);
                    var dbInfo = JsonDeserializationServer.DatabaseInfo(bjro);
                    Assert.Equal(IndexRunningStatus.Running, dbInfo.IndexingStatus);
                }
            }
        }
    }
}
