using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

    protected override IAsyncEnumerable<Document> GetRevisionsAsync(DocumentsOperationContext context)
    {
        if (string.IsNullOrEmpty(_collection))
            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetAllRevisionsOrderedByEtag(context, _start, _pageSize).ToAsyncEnumerable();
        else
            return RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetAllRevisionsOrderedByEtagForCollection(context, _collection, _start, _pageSize).ToAsyncEnumerable();
    }

    protected override async ValueTask InitializeAsync(DocumentsOperationContext context)
    {
        await base.InitializeAsync(context);
        _start = RequestHandler.GetStart();
        _pageSize = RequestHandler.GetPageSize();

        _totalResults = string.IsNullOrEmpty(_collection)
            ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context)
            : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context, _collection);
    }

    protected override IDisposable OpenReadTransaction(DocumentsOperationContext context)
    {
        return context.OpenReadTransaction();
    }

    protected override ValueTask<long> GetTotalCountAsync()
    {
        return ValueTask.FromResult(_totalResults);
    }

    protected override bool NotModified(DocumentsOperationContext context, out string etag)
    {
        string changeVector;
        etag = null;

        changeVector = string.IsNullOrEmpty(_collection)
            ? RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVector(context)
            : RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVector(context, _collection);

        if (changeVector != null)
            etag = $"{changeVector}/{_totalResults}";

        if (etag == null)
            return false;

        if (etag == RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch))
            return true;

        return false;
    }
}

