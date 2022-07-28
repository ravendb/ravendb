using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Sharding.Handlers.Processors.Streaming;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Documents;

internal class ShardedDocumentHandlerProcessorForGet : AbstractDocumentHandlerProcessorForGet<ShardedDocumentHandler, TransactionOperationContext, BlittableJsonReaderObject>
{
    private readonly OperationCancelToken _operationCancelToken;

    public ShardedDocumentHandlerProcessorForGet(HttpMethod method, [NotNull] ShardedDocumentHandler requestHandler) : base(method, requestHandler)
    {
        _operationCancelToken = requestHandler.CreateOperationToken();
    }

    protected override bool SupportsShowingRequestInTrafficWatch => false;

    protected override CancellationToken CancellationToken => _operationCancelToken.Token;

    protected override async ValueTask<DocumentsByIdResult<BlittableJsonReaderObject>> GetDocumentsByIdImplAsync(TransactionOperationContext context, StringValues ids,
        StringValues includePaths, RevisionIncludeField revisions, StringValues counters, HashSet<AbstractTimeSeriesRange> timeSeries, StringValues compareExchangeValues, bool metadataOnly, string etag)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-18754 make sure we maintain the order of returned results");

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "RavenDB-19067 Includes of revisions / counters / time series");

        if (counters.Count > 0)
            throw new NotSupportedInShardingException("Include of counters is not supported");

        if (revisions != null)
            throw new NotSupportedInShardingException("Include of revisions is not supported");

        if (timeSeries != null)
            throw new NotSupportedInShardingException("Include of time series is not supported");

        string[] compareExchangeValuesAsArray = compareExchangeValues;
        var idsByShard = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, ids);
        var op = new FetchDocumentsFromShardsOperation(context, RequestHandler, idsByShard, includePaths, compareExchangeValuesAsArray, etag, metadataOnly);
        var shardedReadResult = await RequestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(idsByShard.Keys.ToArray(), op, CancellationToken);

        var result = new DocumentsByIdResult<BlittableJsonReaderObject>
        {
            Etag = shardedReadResult.CombinedEtag,
            Documents = shardedReadResult.Result?.Documents.Values.ToList(),
            Includes = shardedReadResult.Result?.Includes,
            MissingIncludes = shardedReadResult.Result?.MissingIncludes,
            CompareExchangeIncludes = shardedReadResult.Result?.CompareExchangeValueIncludes,
            StatusCode = (HttpStatusCode)shardedReadResult.StatusCode
        };

        if (result.MissingIncludes?.Count > 0)
        {
            var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, result.MissingIncludes);
            var missingIncludesOp = new FetchDocumentsFromShardsOperation(context, RequestHandler, missingIncludeIdsByShard, includePaths: null, compareExchangeValueIncludes: null, etag: null, metadataOnly: metadataOnly);
            var missingResult = await RequestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, CancellationToken);

            foreach (var (id, missing) in missingResult.Result.Documents)
            {
                if (missing == null)
                    continue;

                result.Includes ??= new List<BlittableJsonReaderObject>();

                result.Includes.Add(missing);
            }

            result.Etag = ComputeHttpEtags.CombineEtags(new[] { result.Etag, missingResult.CombinedEtag });
        }

        return result;
    }

    protected override async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer, TransactionOperationContext context, string propertyName, List<BlittableJsonReaderObject> documentsToWrite,
        bool metadataOnly, CancellationToken token)
    {
        await writer.WriteArrayAsync(propertyName, documentsToWrite);

        return (documentsToWrite.Count, -1);
    }

    protected override async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, TransactionOperationContext context, string propertyName, List<BlittableJsonReaderObject> includes, CancellationToken token)
    {
        writer.WritePropertyName(propertyName);

        await writer.WriteIncludesAsync(includes, token);
    }

    protected override async ValueTask<DocumentsResult> GetDocumentsImplAsync(TransactionOperationContext context, long? etag, StartsWithParams startsWith, string changeVector)
    {
        var token = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context);

        if (etag != null)
            throw new NotSupportedException("Passing etag to a sharded database is not supported");

        ShardedStreamDocumentsOperation op;

        if (startsWith != null)
        {
            op = new ShardedStreamDocumentsOperation(HttpContext, startsWith.IdPrefix, startsWith.Matches, startsWith.Exclude, startsWith.StartAfterId,
                changeVector, token);
        }
        else // recent docs
        {
            op = new ShardedStreamDocumentsOperation(HttpContext, changeVector, token);
        }

        var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, CancellationToken);
        if (results.Result == null)
        {
            Debug.Assert(results.StatusCode == (int)HttpStatusCode.NotModified, $"Got no result but the status code was: {results.StatusCode}");
            return new DocumentsResult
            {
                DocumentsAsync = null,
                ContinuationToken = token,
                Etag = results.CombinedEtag
            };
        }

        var streams = await results.Result.InitializeAsync(RequestHandler.DatabaseContext, CancellationToken);

        Disposables.Add(streams);

        var documents = RequestHandler.DatabaseContext.Streaming.GetDocumentsAsync(streams, token);

        return new DocumentsResult
        {
            DocumentsAsync = documents,
            ContinuationToken = token,
            Etag = results.CombinedEtag
        };
    }
}
