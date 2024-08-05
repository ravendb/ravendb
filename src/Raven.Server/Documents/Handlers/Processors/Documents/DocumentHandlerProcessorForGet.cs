using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal sealed class DocumentHandlerProcessorForGet : AbstractDocumentHandlerProcessorForGet<DocumentHandler, DocumentsOperationContext, Document>
{
    public DocumentHandlerProcessorForGet(HttpMethod method, [NotNull] DocumentHandler requestHandler) : base(method, requestHandler)
    {
    }

    protected override bool SupportsShowingRequestInTrafficWatch => true;

    protected override CancellationToken CancellationToken => RequestHandler.Database.DatabaseShutdown;

    protected override ValueTask<DocumentsByIdResult<Document>> GetDocumentsByIdImplAsync(
        DocumentsOperationContext context,
        List<ReadOnlyMemory<char>> ids,
        StringValues includePaths,
        RevisionIncludeField revisions,
        StringValues counters,
        HashSet<AbstractTimeSeriesRange> timeSeries,
        StringValues compareExchangeValues,
        bool metadataOnly,
        bool clusterWideTx,
        string etag)
    {
        var documents = new List<Document>(ids.Count);
        var includes = new List<Document>(includePaths.Count * ids.Count);
        var includeDocs = new IncludeDocumentsCommand(RequestHandler.Database.DocumentsStorage, context, includePaths, isProjection: false);

        IncludeRevisionsCommand includeRevisions = null;
        IncludeCountersCommand includeCounters = null;
        IncludeTimeSeriesCommand includeTimeSeries = null;
        IncludeCompareExchangeValuesCommand includeCompareExchangeValues = null;

        if (revisions != null)
            includeRevisions = new IncludeRevisionsCommand(RequestHandler.Database, context, revisions);

        if (counters.Count > 0)
        {
            if (counters is [Constants.Counters.All])
                counters = Array.Empty<string>();

            includeCounters = new IncludeCountersCommand(RequestHandler.Database, context, counters);
        }

        if (timeSeries != null)
            includeTimeSeries = new IncludeTimeSeriesCommand(context, new Dictionary<string, HashSet<AbstractTimeSeriesRange>> { { string.Empty, timeSeries } });

        if (compareExchangeValues.Count > 0 || clusterWideTx)
        {
            includeCompareExchangeValues = IncludeCompareExchangeValuesCommand.InternalScope(RequestHandler.Database, compareExchangeValues);
            Disposables.Add(includeCompareExchangeValues);
        }

        long lastModifiedIndex = RequestHandler.Database.ClusterWideTransactionIndexWaiter.LastIndex;
        context.OpenReadTransaction();

        foreach (var id in ids)
        {
            Document document = null;

            if (id.IsEmpty == false)
            {
                document = RequestHandler.Database.DocumentsStorage.Get(context, id);
            }

            if (document == null)
            {
                if (clusterWideTx)
                {
                    Debug.Assert(includeCompareExchangeValues != null, nameof(includeCompareExchangeValues) + " != null");
                    includeCompareExchangeValues.AddDocument(ClusterWideTransactionHelper.GetAtomicGuardKey(id));
                    continue;
                }

                if (ids.Count == 1)
                {
                    return new ValueTask<DocumentsByIdResult<Document>>(new DocumentsByIdResult<Document>
                    {
                        StatusCode = HttpStatusCode.NotFound,
                        Etag = HttpCache.NotFoundResponse
                    });
                }
            }
            else
            {
                if (clusterWideTx)
                {
                    var changeVector = context.GetChangeVector(document.ChangeVector);
                    if (changeVector.Version.Contains(RequestHandler.Database.ClusterTransactionId) == false)
                    {
                        Debug.Assert(includeCompareExchangeValues != null, nameof(includeCompareExchangeValues) + " != null");
                        if (includeCompareExchangeValues.TryGetCompareExchange(ClusterWideTransactionHelper.GetAtomicGuardKey(id), lastModifiedIndex, out var index, out _))
                        {
                            var (isValid, cv) = ChangeVectorUtils.TryUpdateChangeVector(ChangeVectorParser.TrxnTag, RequestHandler.Database.ClusterTransactionId, index, changeVector);
                            Debug.Assert(isValid, "ChangeVector didn't have ClusterTransactionId tag but now does?!");
                            document.ChangeVector = cv;
                        }
                    }
                }
            }

            documents.Add(document);
            includeDocs.Gather(document);
            includeCounters?.Fill(document);
            includeRevisions?.Fill(document);
            includeTimeSeries?.Fill(document);
            includeCompareExchangeValues?.Gather(document);
        }

        includeDocs.Fill(includes, includeMissingAsNull: false);
        includeCompareExchangeValues?.Materialize(lastModifiedIndex);

        var actualEtag = ComputeHttpEtags.ComputeEtagForDocuments(documents, includes, includeCounters, includeTimeSeries, includeCompareExchangeValues);

        if (clusterWideTx)
        {
            Debug.Assert(includeCompareExchangeValues != null, "includeCompareExchangeValues != null");

            if (includeCompareExchangeValues.Results is { Count: > 0 })
            {
                foreach (var (k, v) in includeCompareExchangeValues.Results)
                {
                    if (v.Index >= 0)
                        v.ChangeVector = ChangeVectorUtils.NewChangeVector(ChangeVectorParser.TrxnTag, v.Index, RequestHandler.Database.ClusterTransactionId);
                }
            }
        }

        return new ValueTask<DocumentsByIdResult<Document>>(new DocumentsByIdResult<Document>
        {
            Etag = actualEtag,
            Documents = documents,
            Includes = includes,
            RevisionIncludes = includeRevisions,
            CounterIncludes = includeCounters,
            TimeSeriesIncludes = includeTimeSeries,
            CompareExchangeIncludes = includeCompareExchangeValues?.Results
        });
    }

    protected override async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer,
        DocumentsOperationContext context, IEnumerable<Document> documentsToWrite, bool metadataOnly, CancellationToken token)
    {
        return await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly, token).ConfigureAwait(false);
    }

    protected override async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer,
        DocumentsOperationContext context, IAsyncEnumerable<Document> documentsToWrite, bool metadataOnly, CancellationToken token)
    {
        return await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly, token).ConfigureAwait(false);
    }

    protected override async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName, List<Document> includes, CancellationToken token)
    {
        writer.WritePropertyName(propertyName);

        await writer.WriteIncludesAsync(context, includes, token).ConfigureAwait(false);
    }

    protected override ValueTask<DocumentsResult> GetDocumentsImplAsync(DocumentsOperationContext context, long? etag, StartsWithParams startsWith, string changeVector)
    {
        context.OpenReadTransaction();

        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

        if (changeVector == databaseChangeVector)
        {
            return new ValueTask<DocumentsResult>(new DocumentsResult
            {
                Etag = databaseChangeVector
            });
        }

        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        IEnumerable<Document> documents;
        if (etag != null)
        {
            documents = RequestHandler.Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
        }
        else if (startsWith != null)
        {
            documents = RequestHandler.Database.DocumentsStorage.GetDocumentsStartingWith(context, startsWith.IdPrefix, startsWith.Matches, startsWith.Exclude,
                startsWith.StartAfterId, start, pageSize);
        }
        else // recent docs
        {
            documents = RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
        }

        return new ValueTask<DocumentsResult>(new DocumentsResult
        {
            Documents = documents,
            Etag = databaseChangeVector
        });
    }
}
