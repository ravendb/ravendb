using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Data.Collection;
using Raven.Client.Data.Queries;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Exceptions;
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


        public IOperationResult ExecuteDelete(string collectionName, CollectionOperationOptions options, DocumentsOperationContext documentsOperationContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, _context, onProgress, key => _database.DocumentsStorage.Delete(_context, key, null), token);
        }

        public IOperationResult ExecutePatch(string collectionName, CollectionOperationOptions options, PatchRequest patch, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, _context, onProgress, key => _database.Patch.Apply(context, key, null, patch, null), token);
        }

        private IOperationResult ExecuteOperation(string collectionName, CollectionOperationOptions options, DocumentsOperationContext context, 
             Action<DeterminateProgress> onProgress, Action<string> action, OperationCancelToken token)
        {
            const int BatchSize = 1024;
            var progress = new DeterminateProgress();

            long lastEtag;
            long totalCount;
            using (context.OpenReadTransaction())
            {
                lastEtag = _database.DocumentsStorage.GetLastDocumentEtag(context, collectionName);
                _database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, collectionName, 0, out totalCount);
            }
            progress.Total = totalCount;
            long startEtag = 0;
            using (
                var rateGate = options.MaxOpsPerSecond.HasValue
                    ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1))
                    : null)
            {
                bool done = false;
                //The reason i do this nested loop is because i can't operate on a document while iterating the document tree.
                while (startEtag <= lastEtag)
                {
                    token.Token.ThrowIfCancellationRequested();
                    bool wait = false;

                    using (var tx = context.OpenWriteTransaction())
                    {
                        var documents = _database.DocumentsStorage.GetDocumentsFrom(context, collectionName, startEtag,
                            0, BatchSize).ToList();
                        foreach (var document in documents)
                        {                                
                            token.Token.ThrowIfCancellationRequested();
                            
                            if (document.Etag > lastEtag)
                            {
                                done = true;
                                break;
                            }

                            if (rateGate != null && rateGate.WaitToProceed(0) == false)
                            {
                                wait = true;
                                break;
                            }

                            startEtag = document.Etag;

                            action(document.Key);

                            progress.Processed++;

                        }

                        tx.Commit();

                        onProgress(progress);
 
                        if (wait)
                            rateGate.WaitToProceed();
                        if (done || documents.Count == 0)
                            break;
                    }                        
                }
            }            

            return new BulkOperationResult
            {
                Total = progress.Processed
            };
        }
    }
}
