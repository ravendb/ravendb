using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions
{
    public class RevisionsSubscriptionProcessor : SubscriptionProcessor<RevisionRecord>
    {
        public RevisionsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection,
            SubscriptionConnectionsState subscriptionConnectionsState) :
            base(server, database, connection, subscriptionConnectionsState)
        {
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
            return Database.DocumentsStorage.RevisionsStorage.GetLastRevisionEtag(context, collection);
        }
    }

    public class DocumentsSubscriptionProcessor : SubscriptionProcessor<DocumentRecord>
    {
        public DocumentsSubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection,
            SubscriptionConnectionsState subscriptionConnectionsState) :
            base(server, database, connection, subscriptionConnectionsState)
        {
        }

        public List<string> ItemsToRemoveFromResend = new List<string>();

        public override Task<long> RecordBatch(string lastChangeVectorSentInThisBatch)
        {
            // we skipped all documents, but we still need to update the change vector
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
                    if (ShouldAddToResendTable(doc.DocumentId, document, doc.ChangeVector) == false)
                    {
                        BatchItems.RemoveAt(index);
                    }
                }
            }

            await SubscriptionConnectionsState.AcknowledgeBatch(Connection, batchId, BatchItems);

            if (BatchItems?.Count > 0)
            {
                SubscriptionConnectionsState.NotifyHasMoreDocs();
            }
        }

        public override long GetLastItemEtag(DocumentsOperationContext context, string collection)
        {
            return Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
        }

        private bool ShouldAddToResendTable(string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                switch (Database.DocumentsStorage.GetConflictStatus(item.Document.ChangeVector, currentChangeVector))
                {
                    case ConflictStatus.Update:
                        return true;

                    case ConflictStatus.AlreadyMerged:
                    case ConflictStatus.Conflict:
                        return false;

                    default:
                        throw new InvalidEnumArgumentException();
                }
            }

            return false;
        }
    }

    public abstract class SubscriptionProcessor<T> : SubscriptionProcessor
    {
        protected SubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection,
            SubscriptionConnectionsState subscriptionConnectionsState) :
            base(server, database, connection, subscriptionConnectionsState)
        {
        }

        public List<T> BatchItems = new List<T>();

        public void AddItem(T item) => BatchItems.Add(item);
    }

    public abstract class SubscriptionProcessor
    {
        protected readonly ServerStore Server;
        protected readonly DocumentDatabase Database;
        protected readonly SubscriptionConnectionsState SubscriptionConnectionsState;
        protected readonly SubscriptionConnection Connection;

        protected SubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection,
            SubscriptionConnectionsState subscriptionConnectionsState)
        {
            Server = server;
            Database = database;
            Connection = connection;
            SubscriptionConnectionsState = subscriptionConnectionsState;
        }

        public abstract Task<long> RecordBatch(string lastChangeVectorSentInThisBatch);

        public abstract Task AcknowledgeBatch(long batchId);

        public abstract long GetLastItemEtag(DocumentsOperationContext context, string collection);
    }
}
