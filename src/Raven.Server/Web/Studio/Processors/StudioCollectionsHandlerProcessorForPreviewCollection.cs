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
    private readonly DocumentDatabase _database;

    private int _start;

    private int _pageSize;

    private long _totalResults;

    private IDisposable _releaseContext;

    private IDisposable _closeTransaction;

    private DocumentsOperationContext _context;

    private List<Document> _documents;

    public StudioCollectionsHandlerProcessorForPreviewCollection(DatabaseRequestHandler requestHandler, DocumentDatabase database)
        : base(requestHandler)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        _start = RequestHandler.GetStart();
        _pageSize = RequestHandler.GetPageSize();

        _releaseContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
        _closeTransaction = _context.OpenReadTransaction();

        _totalResults = IsAllDocsCollection
            ? _database.DocumentsStorage.GetNumberOfDocuments(_context)
            : _database.DocumentsStorage.GetCollection(Collection, _context).Count;
    }

    protected override JsonOperationContext GetContext()
    {
        return _context;
    }

    protected override ValueTask<long> GetTotalResultsAsync()
    {
        return ValueTask.FromResult(_totalResults);
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
            changeVector = _database.DocumentsStorage.GetLastDocumentChangeVector(_context.Transaction.InnerTransaction, _context, Collection);

            if (changeVector != null)
                etag = $"{changeVector}/{_totalResults}";
        }

        if (etag == null)
            return false;

        if (etag == RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch))
            return true;

        return false;
    }

    protected override IAsyncEnumerable<Document> GetDocumentsAsync()
    {
        var documents = IsAllDocsCollection
            ? _database.DocumentsStorage
                .GetDocumentsInReverseEtagOrder(_context, _start, _pageSize)
                .ToList()
            : _database.DocumentsStorage
                .GetDocumentsInReverseEtagOrder(_context, Collection, _start, _pageSize)
                .ToList();

        _documents = documents;

        return _documents.ToAsyncEnumerable();
    }

    protected override ValueTask<List<string>> GetAvailableColumnsAsync()
    {
        return ValueTask.FromResult(ExtractColumnNames(_documents, _context));
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
