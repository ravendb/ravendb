using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Schemas;
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
    }

    protected override async Task InitializeAsync(DocumentsOperationContext context, CancellationToken token)
    {
        await base.InitializeAsync(context, token);

        _start = RequestHandler.GetStart();
        _pageSize = RequestHandler.GetPageSize();

        switch (Type)
        {
            case RevisionsType.All:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
                    : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocumentsForCollection(context, Collection);
                break;

            case RevisionsType.NotDeleted:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfNonDeletedRevisions(context)
                    : -1; // Not available for specific collection
                break;

            case RevisionsType.Deleted:
                _totalResults = string.IsNullOrEmpty(Collection)
                    ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context) -
                      RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfNonDeletedRevisions(context)
                    : -1; // Not available for specific collection
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Type), $"Unsupported revision type: {Type}");
        }
    }

    protected override IDisposable OpenReadTransaction(DocumentsOperationContext context)
    {
        return context.OpenReadTransaction();
    }

    protected override Task WriteItemsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteStartArray();
        if (_totalResults != 0)
        {
            IEnumerable<Document> revisions;
            switch (Type)
            {
                case RevisionsType.All:
                    revisions = string.IsNullOrEmpty(Collection)
                        ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrder(context, _start, _pageSize)
                        : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrderForCollection(context, Collection, _start, _pageSize);
                    break;

                case RevisionsType.NotDeleted:
                    revisions = string.IsNullOrEmpty(Collection)
                        ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNotDeletedRevisionsInReverseEtagOrder(context, _start, _pageSize)
                        : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNotDeletedRevisionsInReverseEtagOrderForCollection(context, Collection, _start,
                            _pageSize);
                    break;

                case RevisionsType.Deleted:
                    revisions = string.IsNullOrEmpty(Collection)
                        ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetDeletedRevisionsInReverseEtagOrder(context, _start, _pageSize)
                        : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetDeletedRevisionsInReverseEtagOrderForCollection(context, Collection, _start,
                            _pageSize);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(Type), $"Unsupported revision type: {Type}");
            }

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

