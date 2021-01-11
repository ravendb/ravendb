using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents
{
    internal class CollectionRunner
    {
        internal const int OperationBatchSize = 1024;

        private readonly IndexQueryServerSide _collectionQuery;
        private IndexQueryServerSide _operationQuery;

        protected readonly DocumentsOperationContext Context;
        protected readonly DocumentDatabase Database;

        public CollectionRunner(DocumentDatabase database, DocumentsOperationContext context, IndexQueryServerSide collectionQuery)
        {
            Debug.Assert(collectionQuery == null || collectionQuery.Metadata.IsCollectionQuery);

            Database = database;
            Context = context;
            _collectionQuery = collectionQuery;
        }

        public virtual Task<IOperationResult> ExecuteDelete(string collectionName, long start, long take, CollectionOperationOptions options, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, start, take, options, Context, onProgress, key => new DeleteDocumentCommand(key, null, Database), token);
        }

        public Task<IOperationResult> ExecutePatch(string collectionName, long start, long take, CollectionOperationOptions options, PatchRequest patch,
            BlittableJsonReaderObject patchArgs, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(collectionName, start, take, options, Context, onProgress,
                key => new PatchDocumentCommand(Context, key, null, false, (patch, patchArgs), (null, null), Database, false, false, false, false), token);
        }

        protected async Task<IOperationResult> ExecuteOperation(string collectionName, long start, long take, CollectionOperationOptions options, DocumentsOperationContext context,
             Action<DeterminateProgress> onProgress, Func<string, TransactionOperationsMerger.MergedTransactionCommand> action, OperationCancelToken token)
        {
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
            var internalQueryOperationStart = 0;

            using (var rateGate = options.MaxOpsPerSecond.HasValue
                    ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1))
                    : null)
            {
                var end = false;
                var ids = new Queue<string>(OperationBatchSize);

                while (startEtag <= lastEtag)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    ids.Clear();
                    using (context.OpenReadTransaction())
                    {
                        foreach (var document in GetDocuments(context, collectionName, startEtag, internalQueryOperationStart, OperationBatchSize, isAllDocs, DocumentFields.Id))
                        {
                            using (document)
                            {
                                internalQueryOperationStart++;

                                cancellationToken.ThrowIfCancellationRequested();

                                token.Delay();

                                if (isAllDocs && document.Id.StartsWith(HiLoHandler.RavenHiloIdPrefix, StringComparison.OrdinalIgnoreCase))
                                    continue;

                                if (document.Etag > lastEtag) // we don't want to go over the documents that we have patched
                                {
                                    end = true;
                                    break;
                                }

                                startEtag = document.Etag + 1;

                                if (start > 0)
                                {
                                    start--;
                                    continue;
                                }

                                if (take-- <= 0)
                                {
                                    end = true;
                                    break;
                                }

                                ids.Enqueue(document.Id);
                            }
                        }
                    }

                    if (ids.Count == 0)
                        break;

                    do
                    {
                        var command = new ExecuteRateLimitedOperations<string>(ids, action, rateGate, token,
                            maxTransactionSize: 16 * Voron.Global.Constants.Size.Megabyte,
                            batchSize: OperationBatchSize);

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

            return new BulkOperationResult
            {
                Total = progress.Processed
            };
        }

        protected IEnumerable<Document> GetDocuments(DocumentsOperationContext context, string collectionName, long startEtag, long start, int batchSize, bool isAllDocs, DocumentFields fields)
        {
            if (_collectionQuery != null && _collectionQuery.Metadata.WhereFields.Count > 0)
            {
                if (_operationQuery == null)
                    _operationQuery = ConvertToOperationQuery(_collectionQuery, batchSize);

                return new CollectionQueryEnumerable(Database, Database.DocumentsStorage, new FieldsToFetch(_operationQuery, null),
                    collectionName, _operationQuery, null, context, null, null, new Reference<int>())
                {
                    Fields = fields,
                    InternalQueryOperationStart = start
                };
            }

            if (isAllDocs)
                return Database.DocumentsStorage.GetDocumentsFrom(context, startEtag, 0, batchSize, fields);

            return Database.DocumentsStorage.GetDocumentsFrom(context, collectionName, startEtag, 0, batchSize, fields);
        }

        protected long GetTotalCountForCollection(DocumentsOperationContext context, string collectionName, bool isAllDocs)
        {
            if (isAllDocs)
            {
                var allDocsCount = Database.DocumentsStorage.GetNumberOfDocuments(context);
                Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, CollectionName.HiLoCollection, 0, out long hiloDocsCount);
                return allDocsCount - hiloDocsCount;
            }

            Database.DocumentsStorage.GetNumberOfDocumentsToProcess(context, collectionName, 0, out long totalCount);
            return totalCount;
        }

        protected long GetLastEtagForCollection(DocumentsOperationContext context, string collection, bool isAllDocs)
        {
            if (isAllDocs)
                return DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
        }

        private static IndexQueryServerSide ConvertToOperationQuery(IndexQueryServerSide query, int pageSize)
        {
            return new IndexQueryServerSide(query.Metadata)
            {
                Query = query.Query,
                Start = 0,
                WaitForNonStaleResultsTimeout = query.WaitForNonStaleResultsTimeout,
                PageSize = int.MaxValue,
                QueryParameters = query.QueryParameters
            };
        }
    }
}
