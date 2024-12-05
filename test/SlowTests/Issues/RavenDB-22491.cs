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
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Documents.Handlers.RevisionsHandler;

namespace SlowTests.Issues;

public class RavenDB_22491 : RavenTestBase
{
    public RavenDB_22491(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Studio)]
    public async Task RemoveDefaultConfig_ThenChangingDoc_ShouldDeleteRevisions()
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

        cvs.Add("Non Existing Change Vector");

        var results = await store.Maintenance.SendAsync(new GetRevisionsSizeOperation(cvs));
        Assert.NotNull(results);
        Assert.NotNull(results.Sizes);
        Assert.Equal(3, results.Sizes.Length);

        var newSize = results.Sizes[0].ActualSize;
        var oldSize = results.Sizes[1].ActualSize;

        Assert.True(newSize > oldSize);

        Assert.False(results.Sizes[2].Exist);
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class GetRevisionsSizeOperation : IMaintenanceOperation<GetRevisionsSizeOperation.Results>
    {
        private readonly GetRevisionsSizeParameters _parameters;

        public class Results
        {
            public RevisionSizeDetails[] Sizes { get; set; }
        }

        public static readonly Func<BlittableJsonReaderObject, Results> ToResults = JsonDeserializationClient.GenerateJsonDeserializationRoutine<Results>();

        public GetRevisionsSizeOperation(List<string> changeVectors)
        {
            _parameters = new GetRevisionsSizeParameters { ChangeVectors = changeVectors };
        }

        public RavenCommand<Results> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRevisionsMetadataAndMetricsCommand(_parameters);
        }

        private class GetRevisionsMetadataAndMetricsCommand : RavenCommand<Results>
        {
            private readonly GetRevisionsSizeParameters _parameters;

            public GetRevisionsMetadataAndMetricsCommand(GetRevisionsSizeParameters parameters)
            {
                _parameters = parameters;
            }

            public override bool IsReadRequest { get; }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions/size";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    Content = new BlittableJsonContent(async stream =>
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false))
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = ToResults(response);
            }
        }
    }
}

