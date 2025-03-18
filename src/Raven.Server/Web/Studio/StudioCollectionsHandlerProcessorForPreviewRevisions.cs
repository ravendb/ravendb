using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio;

internal sealed class StudioCollectionsHandlerProcessorForPreviewRevisions : AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<DatabaseRequestHandler, DocumentsOperationContext>
{
    private int _start;
    private int _pageSize;
    private long _totalResults;

    public StudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        _start = RequestHandler.GetStart();
        _pageSize = RequestHandler.GetPageSize();
    }

    protected override Task InitializeAsync(DocumentsOperationContext context, CancellationToken token)
    {
        switch (Type)
        {
            case RevisionsStorage.RevisionType.All:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
                    : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocumentsForCollection(context, Collection);
                break;

            case RevisionsStorage.RevisionType.Regular:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfNonDeletedRevisions(context)
                    : TotalResultsUnsupported; // Not available for specific collection
                break;

            case RevisionsStorage.RevisionType.Deleted:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context) -
                      RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfNonDeletedRevisions(context)
                    : TotalResultsUnsupported; // Not available for specific collection
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Type), $"Unsupported revision type: {Type}");
        }

        return Task.CompletedTask;
    }

    protected override IDisposable OpenReadTransaction(DocumentsOperationContext context)
    {
        return context.OpenReadTransaction();
    }

    protected override Task WriteItemsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteStartArray();

        // '_totalResults' has 3 cases
        //    0 - no results, so we we'll write an empty array on the writer
        //    -1 - unknown number of results (TotalResultsUnsupported), we'll write the revisions info on the writer
        //    else - we have results,  we'll write the revisions info on the writer

        if (_totalResults != 0)
        {
            IEnumerable<Document> revisions = string.IsNullOrEmpty(Collection)
                ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrder(context, Type, _start, _pageSize)
                : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrderForCollection(context, Type, Collection, _start, _pageSize);

            WriteItemsInternal(context, writer, revisions);
        }

        writer.WriteEndArray();
        return Task.CompletedTask;
    }

    private void WriteItemsInternal(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, IEnumerable<Document> revisions)
    {
        Func<DocumentsOperationContext, Document, string> getCollection = string.IsNullOrEmpty(Collection)
            ? (ctx, revision) => RequestHandler.Database.DocumentsStorage.ExtractCollectionName(ctx, revision.Data).Name
            : (ctx, revision) => Collection;

        var first = true;
        foreach (var revision in revisions)
        {
            if (first)
                first = false;
            else
                writer.WriteComma();

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(Document.Id));
            writer.WriteString(revision.Id);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.Etag));
            writer.WriteInteger(revision.Etag);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.LastModified));
            writer.WriteDateTime(revision.LastModified, true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.ChangeVector));
            writer.WriteString(revision.ChangeVector);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.Flags));
            writer.WriteString(revision.Flags.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(Collection));
            writer.WriteString(getCollection(context, revision));

            writer.WriteEndObject();
        }
    }

    protected override ValueTask<long> GetTotalCountAsync()
    {
        return ValueTask.FromResult(_totalResults);
    }

    protected override bool NotModified(DocumentsOperationContext context, out string etag)
    {
        string changeVector;
        etag = null;

        changeVector = string.IsNullOrEmpty(Collection)
            ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVector(context)
            : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVectorForCollection(context, Collection);

        if (changeVector != null)
            etag = $"{changeVector}/{_totalResults}";

        if (etag == null)
            return false;

        if (etag == RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch))
            return true;

        return false;
    }
}

