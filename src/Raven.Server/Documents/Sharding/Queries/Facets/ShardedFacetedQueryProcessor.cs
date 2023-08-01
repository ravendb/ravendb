using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Queries.Facets;

public sealed class ShardedFacetedQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, FacetedQueryResult>
{
    private Dictionary<string, FacetOptions> _optionsByFacet;

    public ShardedFacetedQueryProcessor(
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        IndexQueryServerSide query,
        long? existingResultEtag,
        CancellationToken token)
        : base(context, requestHandler, query, metadataOnly: false, indexEntriesOnly: false, ignoreLimit: false, existingResultEtag, token)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        HashSet<string> facetSetupDocumentIds = null;

        foreach (var selectField in Query.Metadata.SelectFields)
        {
            if (selectField.IsFacet == false)
                continue;

            var facetField = (FacetField)selectField;

            if (facetField.FacetSetupDocumentId != null)
            {
                facetSetupDocumentIds ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (facetSetupDocumentIds.Add(facetField.FacetSetupDocumentId) == false)
                    continue;

                GetDocumentsResult documentResult = await GetFacetSetupDocument(facetField);

                if (documentResult.Results.Length == 0)
                    throw new DocumentDoesNotExistException($"Facet setup document {facetField.FacetSetupDocumentId} was not found");

                var document = (BlittableJsonReaderObject)documentResult.Results[0];

                Query.QueryParameters.Modifications ??= new DynamicJsonValue(Query.QueryParameters);

                Query.QueryParameters.Modifications[facetField.FacetSetupDocumentId] = document;

                var facetSetupDocument = FacetSetup.Create(facetField.FacetSetupDocumentId, document);

                foreach (var facet in facetSetupDocument.Facets) // options aren't applicable for range facets
                {
                    AddFacetOptions(facet.DisplayFieldName ?? facet.FieldName, facet.Options ?? FacetOptions.Default);
                }
            }
            else if (facetField.Ranges == null || facetField.Ranges.Count == 0) // options aren't applicable for range facets
            {
                if (facetField.Name != null)
                    AddFacetOptions(facetField.Name, facetField.HasOptions ? facetField.GetOptions(Context, Query.QueryParameters) : FacetOptions.Default);
            }
        }

        if (Query.QueryParameters.Modifications != null)
            Query.QueryParameters = Context.ReadObject(Query.QueryParameters, "facet-query-parameters");

        await base.InitializeAsync();

        async Task<GetDocumentsResult> GetFacetSetupDocument(FacetField facetField)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(Context, facetField.FacetSetupDocumentId);

            var result = await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(Context,
                new GetDocumentsCommand(RequestHandler.ShardExecutor.Conventions, facetField.FacetSetupDocumentId, includes: null, metadataOnly: false), shardNumber, Token);

            return result;
        }

        void AddFacetOptions(string name, FacetOptions options)
        {
            _optionsByFacet ??= new Dictionary<string, FacetOptions>();

            _optionsByFacet.Add(name, options);
        }
    }

    public override async Task<FacetedQueryResult> ExecuteShardedOperations(QueryTimingsScope scope)
    {
        using (var queryScope = scope?.For(nameof(QueryTimingsScope.Names.Query)))
        {
            ShardedFacetedQueryOperation operation;
            ShardedReadResult<FacetedQueryResult> shardedReadResult;

            using (var executeScope = queryScope?.For(nameof(QueryTimingsScope.Names.Execute)))
            {
                var commands = GetOperationCommands(executeScope);

                operation = new ShardedFacetedQueryOperation(Query, _optionsByFacet, Context, RequestHandler, commands, ExistingResultEtag?.ToString());

                var shards = GetShardNumbers(commands);

                shardedReadResult = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, operation, Token);
            }

            if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
            {
                return FacetedQueryResult.NotModifiedResult;
            }

            var result = shardedReadResult.Result;

            await WaitForRaftIndexIfNeededAsync(result.AutoIndexCreationRaftIndex, scope);

            using (Query.Metadata.HasIncludeOrLoad ? queryScope?.For(nameof(QueryTimingsScope.Names.Includes)) : null)
            {
                if (operation.MissingDocumentIncludes is { Count: > 0 })
                {
                    await HandleMissingDocumentIncludesAsync(Context, RequestHandler.HttpContext.Request, RequestHandler.DatabaseContext,
                        operation.MissingDocumentIncludes, result, MetadataOnly, Token);
                }
            }

            return result;
        }
    }

    protected override ShardedQueryCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope) => CreateShardedQueryCommand(shardNumber, query, scope);
}
