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
                    var command = new GetAllReplicationItemsCommand(etag: 234, pageSize: 100);
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var results = command.Result;
                    Assert.NotNull(results);
                }
            }
        }

        private class GetAllReplicationItemsCommand : RavenCommand<BlittableJsonReaderArray>
        {
            private readonly long _etag;
            private readonly int _pageSize;

            public override bool IsReadRequest => true;

            public GetAllReplicationItemsCommand(long etag, int pageSize)
            {
                _etag = etag;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/debug/replication/all-items?etag={_etag}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Results", out BlittableJsonReaderArray results) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                Result = results;
            }
        }
    }
}
