using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Util;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Queries.Facets;

public class ShardedFacetedQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, FacetedQueryResult>
{
    private readonly long? _existingResultEtag;

    private readonly string _raftUniqueRequestId;

    private Dictionary<string, FacetOptions> _optionsByFacet;

    public ShardedFacetedQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query,
        long? existingResultEtag, CancellationToken token) : base(context, requestHandler, query, false, false, token)
    {
        _existingResultEtag = existingResultEtag;
        _raftUniqueRequestId = _requestHandler.GetRaftRequestIdFromQuery() ?? RaftIdGenerator.NewId();
    }

    public override async ValueTask InitializeAsync()
    {
        HashSet<string> facetSetupDocumentIds = null;

        foreach (var selectField in _query.Metadata.SelectFields)
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

                _query.QueryParameters.Modifications ??= new DynamicJsonValue(_query.QueryParameters);

                _query.QueryParameters.Modifications[facetField.FacetSetupDocumentId] = document;

                var facetSetupDocument = FacetSetup.Create(facetField.FacetSetupDocumentId, document);

                foreach (var facet in facetSetupDocument.Facets) // options aren't applicable for range facets
                {
                    AddFacetOptions(facet.FieldName, facet.Options ?? FacetOptions.Default);
                }
            }
            else if (facetField.Ranges == null || facetField.Ranges.Count == 0) // options aren't applicable for range facets
            {
                if (facetField.Name != null)
                    AddFacetOptions(facetField.Name, facetField.HasOptions ? facetField.GetOptions(_context, _query.QueryParameters) : FacetOptions.Default);
            }
        }

        if (_query.QueryParameters.Modifications != null)
            _query.QueryParameters = _context.ReadObject(_query.QueryParameters, "facet-query-parameters");

        await base.InitializeAsync();

        async Task<GetDocumentsResult> GetFacetSetupDocument(FacetField facetField)
        {
            int shardNumber = _requestHandler.DatabaseContext.GetShardNumberFor(_context, facetField.FacetSetupDocumentId);

            var result = await _requestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(_context,
                new GetDocumentsCommand(facetField.FacetSetupDocumentId, includes: null, metadataOnly: false), shardNumber, _token);

            return result;
        }

        void AddFacetOptions(string name, FacetOptions options)
        {
            _optionsByFacet ??= new Dictionary<string, FacetOptions>();

            _optionsByFacet.Add(name, options);
        }
    }

    public override async Task<FacetedQueryResult> ExecuteShardedOperations()
    {
        var commands = GetOperationCommands();
        
        var operation = new ShardedFacetedQueryOperation(_optionsByFacet, _context, _requestHandler, commands, _existingResultEtag?.ToString());

        var shardedReadResult = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(commands.Keys.ToArray(), operation, _token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return FacetedQueryResult.NotModifiedResult;
        }

        var result = shardedReadResult.Result;

        if (_isAutoMapReduceQuery && result.RaftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _requestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(result.RaftCommandIndex.Value);
        }

        if (operation.MissingDocumentIncludes is { Count: > 0 })
        {
            await HandleMissingDocumentIncludes(operation.MissingDocumentIncludes, result);
        }

        return result;
    }

    protected override ShardedQueryCommand CreateCommand(BlittableJsonReaderObject query)
    {
        return new ShardedQueryCommand(_context.ReadObject(query, "query"), _query, _metadataOnly, _indexEntriesOnly, _query.Metadata.IndexName,
            canReadFromCache: _existingResultEtag != null, raftUniqueRequestId: _raftUniqueRequestId);
    }
}
