using System;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Changes
{
    public interface IDatabaseChanges : IConnectableChanges<IDatabaseChanges>
    {
        /// <summary>
        /// Subscribe to changes for specified index only.
        /// </summary>
        IObservable<IndexChange> ForIndex(string indexName);

        /// <summary>
        /// Subscribe to changes for specified document only.
        /// </summary>
        IObservable<DocumentChange> ForDocument(string docId);

        /// <summary>
        /// Subscribe to changes for all documents.
        /// </summary>
        IObservable<DocumentChange> ForAllDocuments();

        /// <summary>
        /// Subscribe to changes for specified operation only.
        /// </summary>
        /// <returns></returns>
        IObservable<OperationStatusChange> ForOperationId(long operationId);

        /// <summary>
        /// Subscribe to change for all operation statuses. 
        /// </summary>
        /// <returns></returns>
        IObservable<OperationStatusChange> ForAllOperations();

        /// <summary>
        /// Subscribe to changes for all indexes.
        /// </summary>
        IObservable<IndexChange> ForAllIndexes();

        /// <summary>
        /// Subscribe to changes for all transformers.
        /// </summary>
        IObservable<TransformerChange> ForAllTransformers();

        /// <summary>
        /// Subscribe to changes for all documents that Id starts with given prefix.
        /// </summary>
        IObservable<DocumentChange> ForDocumentsStartingWith(string docIdPrefix);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IObservable<DocumentChange> ForDocumentsInCollection(string collectionName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified collection.
        /// </summary>
        IObservable<DocumentChange> ForDocumentsInCollection<TEntity>();

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservable<DocumentChange> ForDocumentsOfType(string typeName);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservable<DocumentChange> ForDocumentsOfType(Type type);

        /// <summary>
        /// Subscribe to changes for all documents that belong to specified type (Raven-Clr-Type).
        /// </summary>
        IObservable<DocumentChange> ForDocumentsOfType<TEntity>();
    }
}
