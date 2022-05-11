namespace Raven.Client.Documents.Changes;

public interface IIndexChanges<out TChange>
{
    /// <summary>
    /// Subscribe to changes for specified index only.
    /// </summary>
    IChangesObservable<TChange> ForIndex(string indexName);

    /// <summary>
    /// Subscribe to changes for all indexes.
    /// </summary>
    IChangesObservable<TChange> ForAllIndexes();
}
