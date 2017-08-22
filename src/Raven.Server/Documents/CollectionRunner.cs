using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    internal class CollectionRunner
    {
        protected readonly DocumentsOperationContext Context;
        protected readonly DocumentDatabase Database;

        public CollectionRunner(DocumentDatabase database, DocumentsOperationContext context)
        {
            Database = database;
            Context = context;
        }

        public virtual Task<IOperationResult> ExecuteDelete(string collectionName, CollectionOperationOptions options, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, Context, onProgress, key => new DeleteDocumentCommand(key, null, Database), token);
        }

        public Task<IOperationResult> ExecutePatch(string collectionName, CollectionOperationOptions options, PatchRequest patch, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, options, Context, onProgress,
                key => new PatchDocumentCommand(Context, key, null, false,(patch, null),(null,null),
                    Database, false,false), token);
        }

        protected async Task<IOperationResult> ExecuteOperation(string collectionName, CollectionOperationOptions options, DocumentsOperationContext context,
             Action<DeterminateProgress> onProgress, Func<LazyStringValue, TransactionOperationsMerger.MergedTransactionCommand> action, OperationCancelToken token)
        {
            const int batchSize = 1024;
            var progress = new DeterminateProgress();
            var cancellationToken = token.Token;
            var isAllDocs = collectionName == Constants.Documents.Collections.AllDocumentsCollection;

            long lastEtag;
            long totalCount;
            using (context.OpenReadTransaction())
            {
                lastEtag = GetLastEtagForCollection(context, collectionName, isAllDocs);
                totalCount = GetTotalCountForCollection(context, collectionName, isAllDocs);
            }
            progress.Total = totalCount;

            

            // send initial progress with total count set, and 0 as processed count
            onProgress(progress);

            long startEtag = 0;
            using (var rateGate = options.MaxOpsPerSecond.HasValue
                    ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1))
                    : null)
            {
                var end = false;

                while (startEtag <= lastEtag)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using (context.OpenReadTransaction())
                    {
                        var ids = new Queue<LazyStringValue>(batchSize);

                        foreach (var document in GetDocuments(context, collectionName, startEtag, batchSize, isAllDocs))
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            token.Delay();

                            if (isAllDocs && IsSystemDocument(document))
                                continue;
                            
                            if (document.Etag > lastEtag) // we don't want to go over the documents that we have patched
                            {
                                end = true;
                                break;
                            }

                            startEtag = document.Etag + 1;

                            ids.Enqueue(document.Id);
                        }

                        if (ids.Count == 0)
                            break;
                        
                        do
                        {
                            var command = new ExecuteRateLimitedOperations<LazyStringValue>(ids, action, rateGate, token);

                            await Database.TxMerger.Enqueue(command);

                            progress.Processed += command.Processed;

                            onProgress(progress);

                            if (command.NeedWait)
                                rateGate?.WaitToProceed();

                        } while (ids.Count > 0);
                        
                        if (end)
                            break;
                    }
                }
            }

            return new BulkOperationResult
            {
                Total = progress.Processed
            };
        }

        private static unsafe bool IsSystemDocument(Document document)
        {
            return CollectionName.IsSystemDocument(document.Id.Buffer, document.Id.Length, out var _);
        }

        protected virtual IEnumerable<Document> GetDocuments(DocumentsOperationContext context, string collectionName, long startEtag, int batchSize, bool isAllDocs)
        {
            if (isAllDocs)
                return Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, batchSize);

            return Database.DocumentsStorage.GetDocumentsFrom(context, collectionName, startEtag, 0, batchSize);
        }

        protected virtual long GetTotalCountForCollection(DocumentsOperationContext context, string collectionName, bool isAllDocs)
        {
            if (isAllDocs)
            {
                var allDocsCount = Database.DocumentsStorage.GetNumberOfDocuments(context);
                Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, CollectionName.SystemCollection, 0, out long systemDocsCount);

                return allDocsCount - systemDocsCount;
            }

            Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, collectionName, 0, out long totalCount);
            return totalCount;
        }

        protected virtual long GetLastEtagForCollection(DocumentsOperationContext context, string collection, bool isAllDocs)
        {
            if (isAllDocs)
                return DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
        }
    }
}
