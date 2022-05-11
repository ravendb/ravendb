namespace Raven.Client.Documents.Changes;

public interface ICounterChanges<out TChange>
{
    /// <summary>
    /// Subscribe to changes for all counters.
    /// </summary>
    IChangesObservable<TChange> ForAllCounters();

    /// <summary>
    /// Subscribe to changes for all counters with a given name.
    /// </summary>
    IChangesObservable<TChange> ForCounter(string counterName);

    /// <summary>
    /// Subscribe to changes for counter from a given document and with given name.
    /// </summary>
    IChangesObservable<TChange> ForCounterOfDocument(string documentId, string counterName);

    /// <summary>
    /// Subscribe to changes for all counters from a given document.
    /// </summary>
    IChangesObservable<TChange> ForCountersOfDocument(string documentId);
}
