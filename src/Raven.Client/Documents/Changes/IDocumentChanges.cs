namespace Raven.Client.Documents.Changes;

public interface IDocumentChanges<out TChange>
{
    /// <summary>
    /// Subscribe to changes for specified document only.
    /// </summary>
    IChangesObservable<TChange> ForDocument(string docId);

    /// <summary>
    /// Subscribe to changes for all documents.
    /// </summary>
    IChangesObservable<TChange> ForAllDocuments();

    /// <summary>
    /// Subscribe to changes for all documents that Id starts with given prefix.
    /// </summary>
    IChangesObservable<TChange> ForDocumentsStartingWith(string docIdPrefix);

    /// <summary>
    /// Subscribe to changes for all documents that belong to specified collection.
    /// </summary>
    IChangesObservable<TChange> ForDocumentsInCollection(string collectionName);


    /// <summary>
    /// Subscribe to changes for all documents that belong to specified collection.
    /// </summary>
    IChangesObservable<TChange> ForDocumentsInCollection<TEntity>();
}
