using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class RevisionsSubscriptionProcessor : SubscriptionProcessor<(Document Previous, Document Current)>
    {
        public RevisionsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
        }

        public List<RevisionRecord> BatchItems = new List<RevisionRecord>();

        public override IEnumerable<(Document Doc, Exception Exception)> GetBatch()
        {
            if (Database.DocumentsStorage.RevisionsStorage.Configuration == null ||
                Database.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(Collection).Disabled)
                throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {Database.Name} does not have revisions configuration.");

            Size size = default;
            var numberOfDocs = 0;

            BatchItems.Clear();

            foreach (var item in Fetcher.GetEnumerator())
            {
                Debug.Assert(item.Current != null);
                if (item.Current == null)
                    continue; // this shouldn't happened, but in release let's keep running

                size += new Size(item.Current.Data?.Size ?? 0, SizeUnit.Bytes);

                using (var oldCurData = item.Current.Data)
                using (var oldPrevData = item.Previous?.Data)
                using (item.Current)
                using (item.Previous)
                {
                    var result = GetBatchItem(item);
                    using (result.Doc)
                    {
                        if (result.Doc.Data != null)
                        {
                            BatchItems.Add(new RevisionRecord
                            {
                                Current = item.Current.ChangeVector,
                                Previous = item.Previous?.ChangeVector
                            });

                            yield return result;
                        }
                        else
                            yield return result;

                        if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                            yield break;

                        if (++numberOfDocs >= BatchSize)
                            yield break;
                    }
                }
            }
        }

        public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
        {
            return Database.SubscriptionStorage.RecordBatchRevisions(
                SubscriptionConnectionsState.SubscriptionId,
                SubscriptionConnectionsState.SubscriptionName,
                BatchItems,
                SubscriptionConnectionsState.PreviouslyRecordedChangeVector,
                lastChangeVectorSentInThisBatch);
        }

        public override Task AcknowledgeBatch(long batchId)
        {
            return SubscriptionConnectionsState.AcknowledgeBatch(Connection, batchId, null);
        }

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

        protected override bool ShouldSend((Document Previous, Document Current) item, out string reason, out Exception exception, out Document result)
        {
            exception = null;
            reason = null;
            result = item.Current;

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: item.Current.ChangeVector,
                    localAsString: SubscriptionState.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{item.Current.Id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsRevisionInActiveBatch(ClusterContext, item.Current.ChangeVector, Active))
                {
                    reason = $"{item.Current.Id} is in active batch";
                    return false;
                }
            }

            item.Current.EnsureMetadata();
            item.Previous?.EnsureMetadata();

            var transformResult = DocsContext.ReadObject(new DynamicJsonValue
            {
                [nameof(RevisionRecord.Current)] = item.Current.Flags.Contain(DocumentFlags.DeleteRevision) ? null : item.Current.Data,
                [nameof(RevisionRecord.Previous)] = item.Previous?.Data
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
