using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForGet : AbstractDocumentHandlerProcessorForGet<DocumentHandler, DocumentsOperationContext, Document>
{
    public DocumentHandlerProcessorForGet(HttpMethod method, [NotNull] DocumentHandler requestHandler) : base(method, requestHandler)
    {
    }

    protected override bool SupportsShowingRequestInTrafficWatch => true;

    protected override CancellationToken CancellationToken => RequestHandler.Database.DatabaseShutdown;

    protected override async ValueTask ExecuteInternalAsync(DocumentsOperationContext context)
    {
        using (context.OpenReadTransaction())
        {
            await base.ExecuteInternalAsync(context);
        }
    }

    protected override ValueTask<DocumentsByIdResult<Document>> GetDocumentsByIdImplAsync(DocumentsOperationContext context, StringValues ids, StringValues includePaths,
        RevisionIncludeField revisions, StringValues counters, HashSet<AbstractTimeSeriesRange> timeSeries, StringValues compareExchangeValues, bool metadataOnly, string etag)
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
            if (counters.Count == 1 && counters[0] == Constants.Counters.All)
                counters = Array.Empty<string>();

            includeCounters = new IncludeCountersCommand(RequestHandler.Database, context, counters);
        }

        if (timeSeries != null)
            includeTimeSeries = new IncludeTimeSeriesCommand(context, new Dictionary<string, HashSet<AbstractTimeSeriesRange>> { { string.Empty, timeSeries } });

        if (compareExchangeValues.Count > 0)
        {
            includeCompareExchangeValues = IncludeCompareExchangeValuesCommand.InternalScope(RequestHandler.Database, compareExchangeValues);
            Disposables.Add(includeCompareExchangeValues);
        }

        foreach (var id in ids)
        {
            Document document = null;

            if (string.IsNullOrEmpty(id) == false)
            {
                document = RequestHandler.Database.DocumentsStorage.Get(context, id);
            }

            if (document == null && ids.Count == 1)
            {
                return new ValueTask<DocumentsByIdResult<Document>>(new DocumentsByIdResult<Document>
                {
                    StatusCode = HttpStatusCode.NotFound,
                    Etag = HttpCache.NotFoundResponse
                });
            }

            documents.Add(document);
            includeDocs.Gather(document);
            includeCounters?.Fill(document);
            includeRevisions?.Fill(document);
            includeTimeSeries?.Fill(document);
            includeCompareExchangeValues?.Gather(document);
        }

        includeDocs.Fill(includes, RequestHandler.GetBoolFromHeaders(Constants.Headers.Sharded) ?? false);
        includeCompareExchangeValues?.Materialize();

        var actualEtag = ComputeHttpEtags.ComputeEtagForDocuments(documents, includes, includeCounters, includeTimeSeries, includeCompareExchangeValues);

        return new ValueTask<DocumentsByIdResult<Document>>(new DocumentsByIdResult<Document>
        {
            Etag = actualEtag,
            Documents = documents,
            Includes = includes,
            RevisionsChangeVectorIncludes = includeRevisions?.RevisionsChangeVectorResults,
            IdByRevisionsByDateTimeIncludes = includeRevisions?.IdByRevisionsByDateTimeResults,
            CounterIncludes = includeCounters?.Results,
            TimeSeriesIncludes = includeTimeSeries?.Results,
            CompareExchangeIncludes = includeCompareExchangeValues?.Results
        });
    }

    protected override async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName, List<Document> documentsToWrite, bool metadataOnly, CancellationToken token)
    {
        writer.WritePropertyName(propertyName);

        return await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly, token);
    }

    protected override async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName, List<Document> includes, CancellationToken token)
    {
        writer.WritePropertyName(propertyName);

        await writer.WriteIncludesAsync(context, includes, token);
    }

    protected override ValueTask<DocumentsResult> GetDocumentsImplAsync(DocumentsOperationContext context, long? etag, StartsWithParams startsWith, string changeVector)
    {
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
