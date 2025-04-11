using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class DatabasesOverviewAnalysisInfo : IDynamicJson
{
    public HashSet<string> DatabaseNames { get; set; } = [];
    
    public int TotalNumberOfDatabases { get; set; }
    public long TotalNumberOfDocuments { get; set; }
    public long TotalNumberOfRevisions { get; set; }
    public long TotalNumberOfTombstones { get; set; }
    public int TotalNumberOfIndexes { get; set; }
    public int TotalNumberOfStaleIndexes { get; set; }
    public int TotalNumberOfErroredIndexes { get; set; }
    public long TotalNumberOfAttachments { get; set; }
    public long TotalNumberOfConflicts { get; set; }
    public long TotalNumberOfCounterEntries { get; set; }
    public long TotalNumberOfTimeSeriesSegments { get; set; }

    public long TotalSizeOnDiskInBytes { get; set; }
    public long TotalTempBuffersSizeOnDiskInBytes { get; set; }
    
    public string BiggestDatabaseName { get; set; }
    public long BiggestDatabaseSizeOnDiskInBytes { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseNames)] = new DynamicJsonArray(DatabaseNames),
            [nameof(TotalNumberOfDatabases)] = TotalNumberOfDatabases,
            [nameof(TotalNumberOfDocuments)] = TotalNumberOfDocuments,
            [nameof(TotalNumberOfRevisions)] = TotalNumberOfRevisions,
            [nameof(TotalNumberOfTombstones)] = TotalNumberOfTombstones,
            [nameof(TotalNumberOfIndexes)] = TotalNumberOfIndexes,
            [nameof(TotalNumberOfStaleIndexes)] = TotalNumberOfStaleIndexes,
            [nameof(TotalNumberOfErroredIndexes)] = TotalNumberOfErroredIndexes,
            [nameof(TotalNumberOfAttachments)] = TotalNumberOfAttachments,
            [nameof(TotalNumberOfConflicts)] = TotalNumberOfConflicts,
            [nameof(TotalNumberOfCounterEntries)] = TotalNumberOfCounterEntries,
            [nameof(TotalNumberOfTimeSeriesSegments)] = TotalNumberOfTimeSeriesSegments,
            [nameof(TotalSizeOnDiskInBytes)] = TotalSizeOnDiskInBytes,
            [nameof(TotalTempBuffersSizeOnDiskInBytes)] = TotalTempBuffersSizeOnDiskInBytes,
            [nameof(BiggestDatabaseName)] = BiggestDatabaseName,
            [nameof(BiggestDatabaseSizeOnDiskInBytes)] = BiggestDatabaseSizeOnDiskInBytes
        };
    }
}
