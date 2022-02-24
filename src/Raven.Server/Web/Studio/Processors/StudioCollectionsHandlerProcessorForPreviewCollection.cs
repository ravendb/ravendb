using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

public class StudioCollectionsHandlerProcessorForPreviewCollection : AbstractStudioCollectionsHandlerProcessorForPreviewCollection<DatabaseRequestHandler>
{
    private int _start;

    private int _pageSize;

    private long _totalResults;

    private IDisposable _releaseContext;

    private IDisposable _closeTransaction;

    private DocumentsOperationContext _context;

    public StudioCollectionsHandlerProcessorForPreviewCollection(DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override void Initialize()
    {
        base.Initialize();

        _start = RequestHandler.GetStart();
        _pageSize = RequestHandler.GetPageSize();

        _releaseContext = RequestHandler.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
        _closeTransaction = _context.OpenReadTransaction();

        _totalResults = IsAllDocsCollection
            ? RequestHandler.Database.DocumentsStorage.GetNumberOfDocuments(_context)
            : RequestHandler.Database.DocumentsStorage.GetCollection(Collection, _context).Count;
    }

    protected override JsonOperationContext GetContext()
    {
        return _context;
    }

    protected override long GetTotalResults()
    {
        return _totalResults;
    }

    protected override bool NotModified(out string etag)
    {
        string changeVector;
        etag = null;
        if (IsAllDocsCollection)
        {
            changeVector = DocumentsStorage.GetDatabaseChangeVector(_context);
            etag = $"{changeVector}/{_totalResults}";
        }
        else
        {
            changeVector = RequestHandler.Database.DocumentsStorage.GetLastDocumentChangeVector(_context.Transaction.InnerTransaction, _context, Collection);

            if (changeVector != null)
                etag = $"{changeVector}/{_totalResults}";
        }

        if (etag == null)
            return false;

        if (etag == RequestHandler.GetStringFromHeaders("If-None-Match"))
            return true;

        return false;
    }

    protected override ValueTask<List<Document>> GetDocumentsAsync()
    {
        var documents = IsAllDocsCollection
            ? RequestHandler.Database.DocumentsStorage
                .GetDocumentsInReverseEtagOrder(_context, _start, _pageSize)
                .ToList()
            : RequestHandler.Database.DocumentsStorage
                .GetDocumentsInReverseEtagOrder(_context, Collection, _start, _pageSize)
                .ToList();

        return ValueTask.FromResult(documents);
    }

    protected override List<string> GetAvailableColumns(List<Document> documents)
    {
        return ExtractColumnNames(documents, _context);
    }

    public override void Dispose()
    {
        base.Dispose();

        _closeTransaction?.Dispose();
        _closeTransaction = null;

        _releaseContext?.Dispose();
        _releaseContext = null;
    }

    private static List<string> ExtractColumnNames(List<Document> documents, JsonOperationContext context)
    {
        var columns = new List<string>();

        foreach (var document in documents)
        {
            FetchColumnNames(document.Data, columns);
        }

        RemoveMetadata(context, columns);

        return columns;
    }

    private static unsafe void FetchColumnNames(BlittableJsonReaderObject data, List<string> columns)
    {
        using (var buffers = data.GetPropertiesByInsertionOrder())
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < buffers.Size; i++)
            {
                data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                var propName = prop.Name;
                if (columns.Contains(propName) == false)
                {
                    columns.Add(prop.Name);
                }
            }
        }
    }

    private static void RemoveMetadata(JsonOperationContext context, List<string> columns)
    {
        var metadataField = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Key);
        columns.Remove(metadataField);
    }
}
