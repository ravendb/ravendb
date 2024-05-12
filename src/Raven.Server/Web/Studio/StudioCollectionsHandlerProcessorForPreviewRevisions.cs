using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents;
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

        _totalResults = string.IsNullOrEmpty(Collection)
            ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
            : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocumentsForCollection(context, Collection);
    }

    protected override IDisposable OpenReadTransaction(DocumentsOperationContext context)
    {
        return context.OpenReadTransaction();
    }

    protected override Task WriteItemsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteStartArray();

        if (_totalResults > 0)
        {
            var revisions = string.IsNullOrEmpty(Collection)
                ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrder(context, _start, _pageSize)
                : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsInReverseEtagOrderForCollection(context, Collection, _start, _pageSize);

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

                writer.WriteEndObject();
            }
        }

        writer.WriteEndArray();
        return Task.CompletedTask;
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

