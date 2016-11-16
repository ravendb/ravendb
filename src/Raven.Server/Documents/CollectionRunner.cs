using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    public class CollectionRunner
    {
        private readonly DocumentsOperationContext _context;
        private readonly DocumentDatabase _database;

        public CollectionRunner(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context = context;
        }


        public IOperationResult ExecuteDelete(string collectionName, DocumentsOperationContext documentsOperationContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, _context, onProgress, key => _database.DocumentsStorage.Delete(_context, key, null), token);
        }

        public IOperationResult ExecutePatch(string collectionName, PatchRequest patch, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, _context, onProgress, key => _database.Patch.Apply(context, key, null, patch, null), token);
        }

        private IOperationResult ExecuteOperation(string collectionName, DocumentsOperationContext context, 
             Action<DeterminateProgress> onProgress, Action<string> action, OperationCancelToken token)
        {
            //TODO:Make a configurable option
            const int batchSize = 1024;
            var progress = new DeterminateProgress();
            RavenTransaction tx = null;
            bool done = false;
            long batchStartEtag = 0;
            try
            {
                long lastEtag;
                long totalCount;
                using (context.OpenReadTransaction())
                {
                    lastEtag = _database.DocumentsStorage.GetLastDocumentEtag(context, collectionName);
                    _database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, collectionName, 0, out totalCount);
                }
                progress.Total = totalCount;
                while (done == false)
                {
                    token.Token.ThrowIfCancellationRequested();
                    List<Document> batch;
                    using (context.OpenReadTransaction())
                    {
                        batch = _database.DocumentsStorage.GetDocumentsFrom(context, collectionName, batchStartEtag,
                            0, batchSize).ToList();
                    }

                    if (batch.Any() == false)
                        break;

                    using (tx = context.OpenWriteTransaction())
                    {
                        foreach (var document in batch)
                        {
                            token.Token.ThrowIfCancellationRequested();
                            batchStartEtag = document.Etag;
                            if (document.Etag > lastEtag)
                            {
                                done = true;
                                break;
                            }
                            action(document.Key);
                            progress.Processed++;

                        }
                        tx.Commit();
                        onProgress(progress);
                    }

                    tx = null;
                    done = progress.Processed == progress.Total;
                }
            }
            finally
            {
                tx?.Dispose();
            }

            return new BulkOperationResult
            {
                Total = progress.Processed
            };
        }
    }
}
