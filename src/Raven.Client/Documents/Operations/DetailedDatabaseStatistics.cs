namespace Raven.Client.Documents.Operations;

public class DetailedDatabaseStatistics : DatabaseStatistics
{
    /// <summary>
    /// Total number of identities in database.
    /// </summary>
    public long CountOfIdentities { get; set; }

    /// <summary>
    /// Total number of compare-exchange values in database.
    /// </summary>
    public long CountOfCompareExchange { get; set; }

    /// <summary>
    /// Total number of compare-exchange tombstones values in database.
    /// </summary>
    public long CountOfCompareExchangeTombstones { get; set; }

    /// <summary>
    /// Total number of TimeSeries Deleted Ranges values in database.
    /// </summary>
    public long CountOfTimeSeriesDeletedRanges { get; set; }
}
