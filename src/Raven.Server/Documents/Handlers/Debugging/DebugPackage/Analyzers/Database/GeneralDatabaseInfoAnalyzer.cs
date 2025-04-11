using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class GeneralDatabaseInfoAnalyzer(
    string databaseName,
    DatabasesOverviewAnalysisInfo databasesOverview,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    public DatabaseAnalysisInfo DatabaseInfo { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries databaseEntries)
    {
        DatabaseStatistics stats;

        try
        {
            if (databaseEntries.TryGetValue<StatsHandler, DatabaseStatistics>(x => x.Stats(), out stats) == false)
            {
                AddWarning($"Could not retrieve database statistics for '{DatabaseName}' database. Skipping it.");
                return false;
            }
        }
        catch (Exception e)
        {
            AddError($"Could not retrieve database statistics for '{DatabaseName}' database. Skipping it.", e);
            return false;
        }

        UpdateDatabasesOverview(stats);
        
        DatabaseInfo = new DatabaseAnalysisInfo
        {
            Stats = stats
        };

        if (databaseEntries.TryGetEntry("/database-record", string.Empty, "json", out var databaseRecordEntry))
        {
            DatabaseInfo.DatabaseRecordEntry = databaseRecordEntry;
            DatabaseInfo.DatabaseRecord = databaseRecordEntry.Deserialize<DatabaseRecord>();
        }
        else
        {
            AddWarning($"Could not retrieve database record for '{DatabaseName}' database. Skipping it.");
        }
        
        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (DatabaseInfo.DatabaseRecord != null)
        {
            var dbRecord = DatabaseInfo.DatabaseRecord;

            if (dbRecord.Encrypted)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Encryption usage",
                    "Database is using encryption",
                    IssueSeverity.Info,
                    IssueCategory.Database));
            }

            var compression = dbRecord.DocumentsCompression;

            if (compression.CompressAllCollections)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Documents Compression usage",
                    "Database is using documents compression for all collections",
                    IssueSeverity.Info,
                    IssueCategory.Database));
            }
            else if (compression.Collections is { Length: > 0 })
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Documents Compression usage",
                    $"Database is using documents compression for the following collections: {string.Join(", ", compression.Collections)}",
                    IssueSeverity.Info,
                    IssueCategory.Database));
            }
        }
    }

    private void UpdateDatabasesOverview(DatabaseStatistics stats)
    {
        databasesOverview.TotalNumberOfDatabases++;
        databasesOverview.DatabaseNames.Add(DatabaseName);
        
        databasesOverview.TotalNumberOfDocuments += stats.CountOfDocuments;
        databasesOverview.TotalNumberOfRevisions += stats.CountOfRevisionDocuments;
        databasesOverview.TotalNumberOfTombstones += stats.CountOfTombstones;

        databasesOverview.TotalNumberOfIndexes += stats.CountOfIndexes;
        databasesOverview.TotalNumberOfStaleIndexes += stats.StaleIndexes.Length;
        databasesOverview.TotalNumberOfErroredIndexes += stats.Indexes.Count(x => x.State == IndexState.Error);

        databasesOverview.TotalNumberOfAttachments += stats.CountOfAttachments;

        databasesOverview.TotalNumberOfConflicts += stats.CountOfConflicts;

        databasesOverview.TotalNumberOfCounterEntries += stats.CountOfCounterEntries;

        databasesOverview.TotalNumberOfTimeSeriesSegments += stats.CountOfTimeSeriesSegments;

        databasesOverview.TotalSizeOnDiskInBytes += stats.SizeOnDisk.SizeInBytes;
        databasesOverview.TotalTempBuffersSizeOnDiskInBytes += stats.TempBuffersSizeOnDisk.SizeInBytes;

        if (stats.SizeOnDisk.SizeInBytes > databasesOverview.BiggestDatabaseSizeOnDiskInBytes)
        {
            databasesOverview.BiggestDatabaseName = DatabaseName;
            databasesOverview.BiggestDatabaseSizeOnDiskInBytes = stats.SizeOnDisk.SizeInBytes;
        }
    }
}
