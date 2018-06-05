using System;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    public interface IDatabaseChanges : IConnectableChanges<IDatabaseChanges>
    {
        /// <summary>
        /// Subscribe to changes for specified index only.
        /// </summary>
        IChangesObservable<IndexChange> ForIndex(string indexName);

        /// <summary>
        /// Subscribe to changes for specified document only.
        /// </summary>
        IChangesObservable<DocumentChange> ForDocument(string docId);

        /// <summary>
        /// Subscribe to changes for all documents.
        /// </summary>
        IChangesObservable<DocumentChange> ForAllDocuments();

        /// <summary>
        /// Subscribe to changes for specified operation only.
        /// </summary>
        /// <returns></returns>
        IChangesObservable<OperationStatusChange> ForOperationId(long operationId);

        /// <summary>
        /// Subscribe to change for all operation statuses. 
        /// </summary>
        /// <returns></returns>
        IChangesObservable<OperationStatusChange> ForAllOperations();

        /// <summary>
        /// Subscribe to changes for all indexes.
        /// </summary>
        IChangesObservable<IndexChange> ForAllIndexes();

        /// <summary>
        /// Subscribe to changes for all documents that Id starts with given prefix.
        /// </summary>
        IChangesObservable<DocumentChange> ForDocumentsStartingWith(string docIdPrefix);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IChangesObservable<DocumentChange> ForDocumentsInCollection(string collectionName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IChangesObservable<DocumentChange> ForDocumentsInCollection<TEntity>();

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        IChangesObservable<DocumentChange> ForDocumentsOfType(string typeName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        IChangesObservable<DocumentChange> ForDocumentsOfType(Type type);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        [Obsolete("This method is not supported anymore. Will be removed in next major version of the product.")]
        IChangesObservable<DocumentChange> ForDocumentsOfType<TEntity>();
    }
}
