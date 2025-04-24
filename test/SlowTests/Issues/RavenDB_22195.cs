using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22195 : ReplicationTestBase
    {
        public RavenDB_22195(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task GetReplicationItemsShouldNotThrowNRE(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                using (var commands = store.Commands())
                {
                    var command = new GetAllReplicationItemsOperation.GetAllReplicationItemsCommand(etag: 234, pageSize: 100);
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var results = command.Result.Results;
                    Assert.NotNull(results);
                }
            }
        }

    }
}
