namespace Raven.Client.Documents.Operations;

public abstract class AbstractDatabaseStatistics<TIndexInformation>
    where TIndexInformation : EssentialIndexInformation
{
    /// <summary>
    /// Total number of indexes in database.
    /// </summary>
    public int CountOfIndexes { get; set; }

    /// <summary>
    /// Total number of documents in database.
    /// </summary>
    public long CountOfDocuments { get; set; }

    /// <summary>
    /// Total number of revision documents in database.
    /// </summary>
    public long CountOfRevisionDocuments { get; set; }

    /// <summary>
    /// Total number of documents conflicts in database.
    /// </summary>
    public long CountOfDocumentsConflicts { get; set; }

    /// <summary>
    /// Total number of tombstones in database.
    /// </summary>
    public long CountOfTombstones { get; set; }

    /// <summary>
    /// Total number of conflicts in database.
    /// </summary>
    public long CountOfConflicts { get; set; }

    /// <summary>
    /// Total number of attachments in database.
    /// </summary>
    public long CountOfAttachments { get; set; }

    /// <summary>
    /// Total number of counter-group entries in database.
    /// </summary>
    public long CountOfCounterEntries { get; set; }

    /// <summary>
    /// Total number of time-series segments in database.
    /// </summary>
    public long CountOfTimeSeriesSegments { get; set; }

    /// <summary>
    /// Statistics for each index in database.
    /// </summary>
    public TIndexInformation[] Indexes { get; set; }
}
