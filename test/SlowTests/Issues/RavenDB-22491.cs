using System;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_22491 : ReplicationTestBase
{
    public RavenDB_22491(ITestOutputHelper output) : base(output)
    {
    }

    // Right Behavior
    [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Studio)]
    public async Task RemoveDefaultConfig_ThenChangingDoc_ShouldDeleteRevisions()
    {
        using var store = GetDocumentStore(new Options()
        {
            ModifyDocumentStore = s => s.Conventions.PreserveDocumentPropertiesNotFoundOnModel = true
        });

        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 100
            }
        };
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

        // Create a doc with 2 revisions
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
            await session.SaveChangesAsync();
        }
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Newwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww" }, "Docs/1");
            await session.SaveChangesAsync();
        }

        var results = await store.Maintenance.SendAsync(new GetRevisionsMetadataAndMetricsOperation("Docs/1", withSize: true, 0, 100));
        Assert.NotNull(results);
        Assert.NotNull(results.Results);
        Assert.Equal(2, results.Results.Length);

        var newSize = results.Results[0].Size.ActualSize;
        var oldSize = results.Results[1].Size.ActualSize;

        Assert.True(newSize > oldSize);

        WaitForUserToContinueTheTest(store, false);

    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class RevisionsWithMetricsResults // Have to be public for GetRevisionsMetadataAndMetricsOperation
    {
        public Result[] Results { get; set; }
    }

    public class Result // Have to be public for GetRevisionsMetadataAndMetricsOperation
    {
        public SizeDetails Size  { get; set; }
    }

    public class GetRevisionsMetadataAndMetricsOperation : IMaintenanceOperation<RevisionsWithMetricsResults>
    {
        private readonly string _id;
        private readonly bool _withSize;
        private readonly int _start;
        private readonly int _pageSize;

        public GetRevisionsMetadataAndMetricsOperation(string id, bool withSize, int start, int pageSize)
        {
            _id = id;
            _withSize = withSize;
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<RevisionsWithMetricsResults> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRevisionsMetadataAndMetricsCommand(_id, _withSize, _start, _pageSize);
        }

        private class GetRevisionsMetadataAndMetricsCommand : RavenCommand<RevisionsWithMetricsResults>
        {
            private readonly string _id;
            private readonly bool _withSize;
            private readonly int _start;
            private readonly int _pageSize;

            private static Func<BlittableJsonReaderObject, RevisionsWithMetricsResults> ToResult = JsonDeserializationClient.GenerateJsonDeserializationRoutine<RevisionsWithMetricsResults>();

            public GetRevisionsMetadataAndMetricsCommand(string id, bool withSize, int start, int pageSize)
            {
                _id = id;
                _withSize = withSize;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions?id={_id}&start={_start}&pageSize={_pageSize}&metadataOnly=true&withSize={_withSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = ToResult(response);
            }
        }
    }

}

