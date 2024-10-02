using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.DataArchival;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions.Processor
{
    public class DocumentsDatabaseSubscriptionProcessor : DatabaseSubscriptionProcessor<Document>
    {
        private readonly CancellationToken _token;

        public List<string> ItemsToRemoveFromResend = new List<string>();
        public List<DocumentRecord> BatchItems = new List<DocumentRecord>();

        public DocumentsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
            _token = connection == null ? CancellationToken.None : connection.CancellationTokenSource.Token;
        }

        public override async Task<SubscriptionBatchResult> GetBatchAsync(SubscriptionBatchStatsScope batchScope, Stopwatch sendingCurrentBatchStopwatch)
        {
            var result = new SubscriptionBatchResult { CurrentBatch = new List<SubscriptionBatchItem>(), LastChangeVectorSentInThisBatch = null };

            BatchItems.Clear();
            ItemsToRemoveFromResend.Clear();

            foreach (var item in Fetcher.GetEnumerator())
            {
                var batchItem = GetBatchItem(item);
                HandleBatchItem(batchScope, batchItem, result, item);

                await SendHeartbeatIfNeededAsync(sendingCurrentBatchStopwatch);
                if (CanContinueBatch(batchItem.Status, batchScope, result.CurrentBatch.Count, sendingCurrentBatchStopwatch) == false)
                    break;
            }

            _token.ThrowIfCancellationRequested();
            result.Status = SetBatchStatus(result);

            return result;
        }

        protected override void HandleBatchItem(SubscriptionBatchStatsScope batchScope, SubscriptionBatchItem batchItem, SubscriptionBatchResult result, Document item)
        {
            if (batchItem.Status == SubscriptionBatchItemStatus.Send || batchItem.Status == SubscriptionBatchItemStatus.Exception)
            {
                // batchItem.Document will be disposed after writing to stream
                BatchItems.Add(new DocumentRecord { DocumentId = batchItem.Document.Id, ChangeVector = batchItem.Document.ChangeVector });

                batchScope?.RecordDocumentInfo(batchItem.Document.Data.Size);

                Connection.TcpConnection.LastEtagSent = batchItem.Document.Etag;

                result.CurrentBatch.Add(batchItem);
            }

            result.LastChangeVectorSentInThisBatch = SetLastChangeVectorInThisBatch(ClusterContext, result.LastChangeVectorSentInThisBatch, batchItem);

            if (batchItem.Status == SubscriptionBatchItemStatus.Skip)
            {
                batchItem.Document.Dispose();
            }
        }

        public override async Task<long> TryRecordBatchAsync(string lastChangeVectorSentInThisBatch) =>
            (await SubscriptionConnectionsState.TryRecordBatchDocumentsAsync(BatchItems, ItemsToRemoveFromResend, lastChangeVectorSentInThisBatch)).Index;

        public override async Task AcknowledgeBatchAsync(long batchId, string changeVector)
        {
            ItemsToRemoveFromResend.Clear();

            //pick up docs that weren't sent due to having been processed by this connection and add them to resend
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docContext))
            using (docContext.OpenReadTransaction())
            {
                for (var index = BatchItems.Count - 1; index >= 0; index--)
                {
                    var doc = BatchItems[index];
                    var document = Database.DocumentsStorage.GetDocumentOrTombstone(docContext, doc.DocumentId, throwOnConflict: false);
                    if (ShouldAddToResendTable(docContext, document, doc.ChangeVector) == false)
                    {
                        BatchItems.RemoveAt(index);
                    }
                }
            }

            await SubscriptionConnectionsState.AcknowledgeBatchAsync(Connection.LastSentChangeVectorInThisConnection
                                                                ?? nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange), batchId, BatchItems);

            if (BatchItems?.Count > 0)
            {
                SubscriptionConnectionsState.NotifyHasMoreDocs();
                BatchItems.Clear();
            }
        }

        public override long GetLastItemEtag(DocumentsOperationContext context, string collection)
        {
            var isAllDocs = collection == Constants.Documents.Collections.AllDocumentsCollection;

            if (isAllDocs)
                return DocumentsStorage.ReadLastDocumentEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
        }


        protected override SubscriptionFetcher<Document> CreateFetcher()
        {
            return new DocumentSubscriptionFetcher(Database, SubscriptionConnectionsState, Collection);
        }

        protected override SubscriptionBatchItem ShouldSend(Document item, out string reason)
        {
            reason = null;
            string id = item.Id; // we convert the Id to string since item might get disposed
            var result = new SubscriptionBatchItem
            {
                Document = item,
                FetchingFrom = Fetcher.FetchingFrom
            };

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                var conflictStatus = GetConflictStatus(item.ChangeVector);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{id} is already merged";
                    result.Status = SubscriptionBatchItemStatus.Skip;
                    return result;
                }

                if (SubscriptionConnectionsState.IsDocumentInActiveBatch(ClusterContext, id, Active))
                {
                    reason = $"{id} exists in an active batch";
                    result.Status = SubscriptionBatchItemStatus.Skip;
                    return result;
                }
            }

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
            {
                var current = Database.DocumentsStorage.Get(DocsContext, id, throwOnConflict: false);
                if (ShouldFetchFromResend(DocsContext, id, current, item.ChangeVector, out reason) == false)
                {
                    result.Document.ChangeVector = string.Empty;
                    current?.Dispose();
                    result.Status = SubscriptionBatchItemStatus.Skip;
                    return result;
                }

                Debug.Assert(current != null, "Document does not exist");

                result.Document.Dispose();
                result.Document = new Document()
                {
                    Data = current.Data, 
                    Id = current.Id, // use proper casing
                    LowerId = current.LowerId, 
                    ChangeVector = current.ChangeVector,
                    Flags = current.Flags
                };
            }

            if (result.Document.Flags.Contain(DocumentFlags.Archived) && SubscriptionState.ArchivedDataProcessingBehavior == ArchivedDataProcessingBehavior.ExcludeArchived)
            {
                reason = $"{id} is archived, while the archived data processing behavior is '{SubscriptionState.ArchivedDataProcessingBehavior}'";
                result.Status = SubscriptionBatchItemStatus.Skip;
                return result;
            }
            
            if (result.Document.Flags.Contain(DocumentFlags.Archived) == false && SubscriptionState.ArchivedDataProcessingBehavior == ArchivedDataProcessingBehavior.ArchivedOnly)
            {
                reason = $"{id} is not archived, while the archived data processing behavior is '{SubscriptionState.ArchivedDataProcessingBehavior}'";
                result.Status = SubscriptionBatchItemStatus.Skip;
                return result;
            }
            
            if (Patch == null)
            {
                result.Status = SubscriptionBatchItemStatus.Send;
                return result;
            }

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, result.Document, ProjectionMetadataModifier.Instance, ref result.Document.Data);

                if (match == false)
                {
                    if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
                    {
                        result.Document.ChangeVector = string.Empty;
                        ItemsToRemoveFromResend.Add(id);
                    }

                    reason = $"{id} filtered out by criteria";
                    result.Status = SubscriptionBatchItemStatus.Skip;
                    return result;
                }

                result.Status = SubscriptionBatchItemStatus.Send;
                return result;
            }
            catch (Exception ex)
            {
                reason = $"Criteria script threw exception for document id {id}";
                result.Exception = ex;
                result.Status = SubscriptionBatchItemStatus.Exception;
                return result;
            }
        }

        protected virtual bool ShouldFetchFromResend(DocumentsOperationContext context, string id, Document item, string currentChangeVector, out string reason)
        {
            reason = null;
            if (item == null)
            {
                // the document was delete while it was processed by the client
                ItemsToRemoveFromResend.Add(id);
                reason = $"document '{id}' removed and skipped from resend";
                return false;
            }

            var status = Database.DocumentsStorage.GetConflictStatus(context, item.ChangeVector, currentChangeVector, ChangeVectorMode.Version);
            switch (status)
            {
                case ConflictStatus.Update:
                    // If document was updated, but the subscription went too far.
                    var resendStatus = Database.DocumentsStorage.GetConflictStatus(context, item.ChangeVector, SubscriptionConnectionsState.LastChangeVectorSent, ChangeVectorMode.Order);
                    if (resendStatus == ConflictStatus.Update)
                    {
                        // we can clear it from resend list, and it will processed as regular document
                        ItemsToRemoveFromResend.Add(id);
                        reason = $"document '{id}' was updated ({item.ChangeVector}), but the subscription went too far and skipped from resend (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";
                        return false;
                    }

                    // We need to resend it
                    var fetch = resendStatus == ConflictStatus.AlreadyMerged;
                    if (fetch == false)
                        reason = $"document '{id}' is in status {resendStatus} (local: {item.ChangeVector}) with the subscription progress (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";

                    return fetch;

                case ConflictStatus.AlreadyMerged:
                    if (CheckIfNewerInResendList(context, item.Id, item.ChangeVector, currentChangeVector))
                    {
                        reason = $"document '{id}' is older in storage (cv: '{item.ChangeVector}') then in resend list (cv: '{currentChangeVector}'), probably there is a active migration. sub progress: {SubscriptionConnectionsState.LastChangeVectorSent}";
                        return false;
                    }

                    return true;

                case ConflictStatus.Conflict:
                    reason = $"document '{id}' is in conflict, CV in storage '{item.ChangeVector}' CV in resend list '{currentChangeVector}' (sub progress: {SubscriptionConnectionsState.LastChangeVectorSent})";
                    return false;

                default:
                    throw new ArgumentOutOfRangeException(nameof(ConflictStatus), status.ToString());
            }
        }

        protected virtual bool CheckIfNewerInResendList(DocumentsOperationContext context, string id, string cvInStorage, string cvInResendList)
        {
            return false;
        }

        private bool ShouldAddToResendTable(DocumentsOperationContext context, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                var status = Database.DocumentsStorage.GetConflictStatus(context, item.Document.ChangeVector, currentChangeVector, ChangeVectorMode.Version);
                switch (status)
                {
                    case ConflictStatus.Update:
                        return true;

                    case ConflictStatus.AlreadyMerged:
                    case ConflictStatus.Conflict:
                        return false;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(ConflictStatus), status.ToString());
                }
            }

            return false;
        }
    }
}
