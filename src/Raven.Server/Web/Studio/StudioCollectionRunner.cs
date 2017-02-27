using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    internal class StudioCollectionRunner : CollectionRunner
    {
        private readonly HashSet<LazyStringValue> _excludeIds;

        public StudioCollectionRunner(DocumentDatabase database, DocumentsOperationContext context, HashSet<LazyStringValue> excludeIds) : base(database, context)
        {
            _excludeIds = excludeIds;
        }

        public override unsafe IOperationResult ExecuteDelete(string collectionName, CollectionOperationOptions options, DocumentsOperationContext documentsOperationContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            if (collectionName == Constants.Documents.Indexing.AllDocumentsCollection)
            {
                bool _;
                if (_excludeIds.Count == 0)
                {
                    // all documents w/o exclusions -> filter system documents
                    return ExecuteOperation(collectionName, options, _context, onProgress, key =>
                    {
                        if (CollectionName.IsSystemDocument(key.Buffer, key.Length, out _) == false)
                        {
                            _database.DocumentsStorage.Delete(_context, key, null);
                        }
                    }, token);
                }
                // all documents w/ exluclusions -> delete only not excluded and not system
                return ExecuteOperation(collectionName, options, _context, onProgress, key =>
                {
                    if (_excludeIds.Contains(key) == false && CollectionName.IsSystemDocument(key.Buffer, key.Length, out _) == false)
                    {
                        _database.DocumentsStorage.Delete(_context, key, null);
                    }
                }, token);
            }

            if (_excludeIds.Count == 0)
                return base.ExecuteDelete(collectionName, options, documentsOperationContext, onProgress, token);

            // specific collection w/ exclusions
            return ExecuteOperation(collectionName, options, _context, onProgress, key =>
            {
                if (_excludeIds.Contains(key) == false)
                {
                    _database.DocumentsStorage.Delete(_context, key, null);
                }
            }, token);
        }

        protected override List<Document> GetDocuments(DocumentsOperationContext context, string collectionName, long startEtag, int batchSize)
        {
            if (collectionName == Constants.Documents.Indexing.AllDocumentsCollection)
                return _database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, batchSize).ToList();

            return base.GetDocuments(context, collectionName, startEtag, batchSize);
        }

        protected override long GetTotalCountForCollection(DocumentsOperationContext context, string collectionName)
        {
            if (collectionName == Constants.Documents.Indexing.AllDocumentsCollection)
                return _database.DocumentsStorage.GetNumberOfDocuments(context);

            return base.GetTotalCountForCollection(context, collectionName);
        }

        protected override long GetLastEtagForCollection(DocumentsOperationContext context, string collection)
        {
            return collection == Constants.Documents.Indexing.AllDocumentsCollection
                ? DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction)
                : base.GetLastEtagForCollection(context, collection);
        }
    }
}