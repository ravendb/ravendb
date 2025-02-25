using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22491 : RavenTestBase
{
    public RavenDB_22491(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Studio)]
    public async Task GetRevisionsSize()
    {
        using var store = GetDocumentStore(new Options() { ModifyDocumentStore = s => s.Conventions.PreserveDocumentPropertiesNotFoundOnModel = true });

        var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

        // Create a doc with 2 revisions
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(
                new User { Name = "Newwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww" }, "Docs/1");
            await session.SaveChangesAsync();
        }

        List<string> cvs;
        using (var session = store.OpenAsyncSession())
        {
            cvs = (await session.Advanced.Revisions.GetMetadataForAsync("Docs/1")).Select(metadata =>
            {
                if (metadata.TryGetValue(Constants.Documents.Metadata.ChangeVector, out string cv) == false)
                    return null;
                return cv;
            }).Where(cv => cv != null).ToList();
        }

        Assert.Equal(2, cvs.Count);

        var result0 = await store.Maintenance.SendAsync(new GetRevisionsSizeOperation(cvs[0]));
        Assert.NotNull(result0);

        var result1 = await store.Maintenance.SendAsync(new GetRevisionsSizeOperation(cvs[1]));
        Assert.NotNull(result0);

        var newSize = result0.ActualSize;
        var oldSize = result1.ActualSize;

        Assert.True(newSize > oldSize);

        var result3 = await store.Maintenance.SendAsync(new GetRevisionsSizeOperation("Non Existing Change Vector"));
        Assert.Null(result3);
        
        var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new GetRevisionsSizeOperation(string.Empty)));
        Assert.StartsWith("System.ArgumentException: Query string value 'changeVector' must have a non empty value", e.Message);
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class GetRevisionsSizeOperation : IMaintenanceOperation<RevisionSizeDetails>
    {
        private string _changeVector;

        private static readonly Func<BlittableJsonReaderObject, RevisionSizeDetails> ToResults = JsonDeserializationClient.GenerateJsonDeserializationRoutine<RevisionSizeDetails>();

        public GetRevisionsSizeOperation(string changeVector)
        {
            _changeVector = changeVector;
        }

        public RavenCommand<RevisionSizeDetails> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRevisionsMetadataAndMetricsCommand(_changeVector);
        }

        private class GetRevisionsMetadataAndMetricsCommand : RavenCommand<RevisionSizeDetails>
        {
            private string _changeVector;


            public GetRevisionsMetadataAndMetricsCommand(string changeVector)
            {
                _changeVector = changeVector;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions/size?changeVector={Uri.EscapeDataString(_changeVector)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                {
                    Result = null;
                    return;
                }

                Result = ToResults(response);
            }
        }
    }
}

