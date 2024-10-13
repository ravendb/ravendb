using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public class DocumentsSubscriptionProcessor : SubscriptionProcessor<Document>
    {
        public DocumentsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
        }

        public override IEnumerable<(Document Doc, Exception Exception)> GetBatch()
        {
            Size size = default;
            var numberOfDocs = 0;

            BatchItems.Clear();
            ItemsToRemoveFromResend.Clear();

            foreach (var item in Fetcher.GetEnumerator())
            {
                using (item)
                {
                    size += new Size(item.Data?.Size ?? 0, SizeUnit.Bytes);

                    var result = GetBatchItem(item);
                    using (result.Doc)
                    {
                        if (result.Doc.Data != null)
                        {
                            BatchItems.Add(new DocumentRecord
                            {
                                DocumentId = result.Doc.Id,
                                ChangeVector = result.Doc.ChangeVector,
                            });

                            yield return result;

                            if (++numberOfDocs >= BatchSize)
                                yield break;
                        }
                        else
                            yield return result;

                        if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                            yield break;
                    }
                }
            }
        }

        public List<string> ItemsToRemoveFromResend = new List<string>();
        public List<DocumentRecord> BatchItems = new List<DocumentRecord>();

        public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
        {
            return Database.SubscriptionStorage.RecordBatchDocuments(
                SubscriptionConnectionsState.SubscriptionId,
                SubscriptionConnectionsState.SubscriptionName,
                BatchItems,
                ItemsToRemoveFromResend,
                SubscriptionConnectionsState.PreviouslyRecordedChangeVector,
                lastChangeVectorSentInThisBatch);
        }

        public override async Task AcknowledgeBatch(long batchId)
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
                    if (ShouldAddToResendTable(document, doc.ChangeVector) == false)
                    {
                        BatchItems.RemoveAt(index);
                    }
                }
            }

            await SubscriptionConnectionsState.AcknowledgeBatch(Connection, batchId, BatchItems);

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

        protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result)
        {
            exception = null;
            reason = null;
            result = item;
            string id = item.Id; // we convert the Id to string since item might get disposed

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Storage)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: item.ChangeVector,
                    localAsString: SubscriptionState.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsDocumentInActiveBatch(ClusterContext, id, Active))
                {
                    reason = $"{id} exists in an active batch";
                    return false;
                }
            }

            if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
            {
                var current = Database.DocumentsStorage.GetDocumentOrTombstone(DocsContext, id, throwOnConflict: false);
                if (ShouldFetchFromResend(id, current, item.ChangeVector) == false)
                {
                    item.ChangeVector = string.Empty;
                    current.Document?.Dispose();
                    current.Tombstone?.Dispose();
                    reason = $"Skip {id} from resend";
                    return false;
                }

                Debug.Assert(current.Document != null, "Document does not exist");

                result.Dispose();
                result = new Document()
                {
                    Id = current.Document.Id, // use proper casing
                    Data = current.Document.Data,
                    LowerId = current.Document.LowerId,
                    Etag = current.Document.Etag,
                    ChangeVector = current.Document.ChangeVector
                };
            }

            if (Patch == null)
                return true;

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, result, ProjectionMetadataModifier.Instance, ref result.Data);

                if (match == false)
                {
                    if (Fetcher.FetchingFrom == SubscriptionFetcher.FetchingOrigin.Resend)
                        ItemsToRemoveFromResend.Add(id);

                    result.Data = null;
                    reason = $"{id} filtered out by criteria";
                    return false;
                }
                
                return true;
            }
            catch (Exception ex)
            {
                exception = ex;
                reason = $"Criteria script threw exception for document id {id}";
                return false;
            }
        }

        private bool ShouldFetchFromResend(string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document == null)
            {
                // the document was delete while it was processed by the client
                ItemsToRemoveFromResend.Add(id);
                return false;
            }

            var status = Database.DocumentsStorage.GetConflictStatusForOrder(item.Document.ChangeVector, currentChangeVector);
            switch (status)
            {
                case ConflictStatus.Update:
                    // If document was updated, but the subscription went too far.
                    var resendStatus = Database.DocumentsStorage.GetConflictStatusForOrder(item.Document.ChangeVector, SubscriptionConnectionsState.LastChangeVectorSent);
                    if (resendStatus == ConflictStatus.Update)
                    {
                        // we can clear it from resend list, and it will processed as regular document
                        ItemsToRemoveFromResend.Add(id);
                        return false;
                    }

                    // We need to resend it
                    return resendStatus == ConflictStatus.AlreadyMerged;

                case ConflictStatus.AlreadyMerged:
                    return true;

                case ConflictStatus.Conflict:
                    return false;

                default:
                    throw new ArgumentOutOfRangeException(nameof(ConflictStatus), status.ToString());
            }
        }

        private bool ShouldAddToResendTable(DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                var status = Database.DocumentsStorage.GetConflictStatusForVersion(item.Document.ChangeVector, currentChangeVector);
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
