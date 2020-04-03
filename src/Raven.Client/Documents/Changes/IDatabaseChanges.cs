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
        /// Subscribe to changes for all counters.
        /// </summary>
        IChangesObservable<CounterChange> ForAllCounters();

        /// <summary>
        /// Subscribe to changes for all counters with a given name.
        /// </summary>
        IChangesObservable<CounterChange> ForCounter(string counterName);

        /// <summary>
        /// Subscribe to changes for counter from a given document and with given name.
        /// </summary>
        IChangesObservable<CounterChange> ForCounterOfDocument(string documentId, string counterName);

        /// <summary>
        /// Subscribe to changes for all counters from a given document.
        /// </summary>
        IChangesObservable<CounterChange> ForCountersOfDocument(string documentId);

        /// <summary>
        /// Subscribe to changes for all timeseries.
        /// </summary>
        IChangesObservable<TimeSeriesChange> ForAllTimeSeries();

        /// <summary>
        /// Subscribe to changes for all timeseries with a given name.
        /// </summary>
        IChangesObservable<TimeSeriesChange> ForTimeSeries(string timeSeriesName);

        /// <summary>
        /// Subscribe to changes for timeseries from a given document and with given name.
        /// </summary>
        IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId, string timeSeriesName);

        /// <summary>
        /// Subscribe to changes for all timeseries from a given document.
        /// </summary>
        IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId);
    }
}
