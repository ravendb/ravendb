using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class RevisionsDatabaseSubscriptionProcessor : DatabaseSubscriptionProcessor<(Document Previous, Document Current)>
    {
        private readonly CancellationToken _token;

        public RevisionsDatabaseSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
            _token = connection == null ? new CancellationToken() : connection.CancellationTokenSource.Token;
    }

        public List<RevisionRecord> BatchItems = new List<RevisionRecord>();
    
        public override async Task<SubscriptionBatchResult> GetBatch(SubscriptionBatchStatsScope batchScope, Stopwatch sendingCurrentBatchStopwatch)
        {
            if (Database.DocumentsStorage.RevisionsStorage.Configuration == null ||
                Database.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(Collection).Disabled)
                throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {Database.Name} does not have revisions configuration.");

            Size size = default;
            var result = new SubscriptionBatchResult { CurrentBatch = new List<BatchItem>(), LastChangeVectorSentInThisBatch = null };

            BatchItems.Clear();

            foreach ((Document Previous, Document Current) item in Fetcher.GetEnumerator())
            {
                Debug.Assert(item.Current != null);
                if (item.Current == null)
                    continue; // this shouldn't happened, but in release let's keep running

                using (item.Current)
                using (item.Previous)
                {
                    BatchItem batchItem = GetBatchItem(item);

                    HandleBatchItem(batchScope, batchItem, result, item);
                    size += new Size(batchItem.Document.Data?.Size ?? 0, SizeUnit.Bytes);
                    if (await CanContinueBatchAsync(batchItem, size, result.CurrentBatch.Count, sendingCurrentBatchStopwatch) == false)
                        break;
                }
            }

            _token.ThrowIfCancellationRequested();
            result.Status = SetBatchStatus(result);

            return result;
        }

        protected override void HandleBatchItem(SubscriptionBatchStatsScope batchScope, BatchItem batchItem, SubscriptionBatchResult result, (Document Previous, Document Current) item)
        {
            if (batchItem.Document.Data != null)
            {
                BatchItems.Add(new RevisionRecord
                {
                    DocumentId = item.Current.Id,
                    Current = item.Current.ChangeVector,
                    Previous = item.Previous?.ChangeVector
                });

                batchScope?.RecordDocumentInfo(batchItem.Document.Data.Size);

                Connection.TcpConnection.LastEtagSent = batchItem.Document.Etag;

                result.CurrentBatch.Add(batchItem);
            }

            result.LastChangeVectorSentInThisBatch = SetLastChangeVectorInThisBatch(ClusterContext, result.LastChangeVectorSentInThisBatch, batchItem);
        }

        public override async Task<long> RecordBatchAsync(string lastChangeVectorSentInThisBatch) => 
            (await SubscriptionConnectionsState.RecordBatchRevisions(BatchItems, lastChangeVectorSentInThisBatch)).Index;

        public override Task AcknowledgeBatchAsync(long batchId, string changeVector) => 
            SubscriptionConnectionsState.AcknowledgeBatchAsync(Connection.LastSentChangeVectorInThisConnection, batchId, null);

        public override long GetLastItemEtag(DocumentsOperationContext context, string collection)
        {
            var isAllDocs = collection == Constants.Documents.Collections.AllDocumentsCollection;

            if (isAllDocs)
                return DocumentsStorage.ReadLastRevisionsEtag(context.Transaction.InnerTransaction);

            return Database.DocumentsStorage.RevisionsStorage.GetLastRevisionEtag(context, collection);
        }

        protected override SubscriptionFetcher<(Document Previous, Document Current)> CreateFetcher()
        {
            return new RevisionSubscriptionFetcher(Database, SubscriptionConnectionsState, Collection);
        }

        protected override bool ShouldSend((Document Previous, Document Current) item, out string reason, out Exception exception, out Document result, out bool isActiveMigration)
        {
            exception = null;
            reason = null;
            isActiveMigration = false;
            result = item.Current.CloneWith(DocsContext, newData: null);

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                var conflictStatus = GetConflictStatus(item.Current.ChangeVector);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{item.Current.Id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsRevisionInActiveBatch(ClusterContext, item.Current, Active))
                {
                    reason = $"{item.Current.Id} is in active batch";
                    return false;
                }
            }

            item.Current.EnsureMetadata();
            item.Previous?.EnsureMetadata();

            var transformResult = DocsContext.ReadObject(new DynamicJsonValue
            {
                ["Current"] = item.Current.Flags.Contain(DocumentFlags.DeleteRevision) ? null : item.Current.Data,
                ["Previous"] = item.Previous?.Data
            }, item.Current.Id);

            result.Data = transformResult;

            if (Patch == null)
                return true;

            item.Current.ResetModifications();
            item.Previous?.ResetModifications();

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, transformResult, ProjectionMetadataModifier.Instance, ref result.Data);
                if (match == false)
                {
                    result.Data.Dispose();
                    result.Data = null;
                    reason = $"{item.Current.Id} filtered by criteria";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                reason = $"Criteria script threw exception for revision id {item.Current.Id} with change vector current: {item.Current.ChangeVector}, previous: {item.Previous?.ChangeVector}";
                exception = ex;
                return false;
            }
        }
    }
}
