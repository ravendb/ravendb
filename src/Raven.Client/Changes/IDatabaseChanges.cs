using System;
using Raven.Client.Data;

namespace Raven.Client.Changes
{
    public interface IDatabaseChanges : IConnectableChanges<IDatabaseChanges>
    {
        /// <summary>
        /// Subscribe to changes for specified index only.
        /// </summary>
        IObservableWithTask<IndexChange> ForIndex(string indexName);

        /// <summary>
        /// Subscribe to changes for specified document only.
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocument(string docId);

        /// <summary>
        /// Subscribe to changes for all documents.
        /// </summary>
        IObservableWithTask<DocumentChange> ForAllDocuments();

        /// <summary>
        /// Subscribe to changes for specified operation only.
        /// </summary>
        /// <returns></returns>
        IObservableWithTask<OperationStatusChange> ForOperationId(long operationId);

        /// <summary>
        /// Subscribe to change for all operation statuses. 
        /// </summary>
        /// <returns></returns>
        IObservableWithTask<OperationStatusChange> ForAllOperations();

        /// <summary>
        /// Subscribe to changes for all indexes.
        /// </summary>
        IObservableWithTask<IndexChange> ForAllIndexes();

        /// <summary>
        /// Subscribe to changes for all transformers.
        /// </summary>
        IObservableWithTask<TransformerChange> ForAllTransformers();

        /// <summary>
        /// Subscribe to changes for all documents that Id starts with given prefix.
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsStartingWith(string docIdPrefix);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsInCollection(string collectionName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsInCollection<TEntity>();

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsOfType(string typeName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsOfType(Type type);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservableWithTask<DocumentChange> ForDocumentsOfType<TEntity>();

        /// <summary>
        /// Subscribe to all replication conflicts.
        /// </summary>
        IObservableWithTask<ReplicationConflictChange> ForAllReplicationConflicts();

        /// <summary>
        /// Subscribe to all bulk insert operation changes that belong to a operation with given Id.
        /// </summary>
        IObservableWithTask<BulkInsertChange> ForBulkInsert(Guid? operationId = null);

        /// <summary>
        /// Subscribe to changes for all data subscriptions.
        /// </summary>
        IObservableWithTask<DataSubscriptionChange> ForAllDataSubscriptions();

        /// <summary>
        /// Subscribe to changes for a specified data subscription.
        /// </summary>
        IObservableWithTask<DataSubscriptionChange> ForDataSubscription(long id);

    }
}
