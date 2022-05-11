namespace Raven.Client.Documents.Changes;

public interface ITimeSeriesChanges<out TChange>
{
    /// <summary>
    /// Subscribe to changes for all timeseries.
    /// </summary>
    IChangesObservable<TChange> ForAllTimeSeries();

    /// <summary>
    /// Subscribe to changes for all timeseries with a given name.
    /// </summary>
    IChangesObservable<TChange> ForTimeSeries(string timeSeriesName);

    /// <summary>
    /// Subscribe to changes for timeseries from a given document and with given name.
    /// </summary>
    IChangesObservable<TChange> ForTimeSeriesOfDocument(string documentId, string timeSeriesName);

    /// <summary>
    /// Subscribe to changes for all timeseries from a given document.
    /// </summary>
    IChangesObservable<TChange> ForTimeSeriesOfDocument(string documentId);
}
