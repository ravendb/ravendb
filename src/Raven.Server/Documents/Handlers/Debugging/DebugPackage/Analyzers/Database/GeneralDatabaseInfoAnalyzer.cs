using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class GeneralDatabaseInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    public DatabaseAnalysisInfo DatabaseInfo { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries databaseEntries)
    {
        DatabaseStatistics stats;

        if (databaseEntries.TryGetEntry("/database-record", string.Empty, "json", out var databaseRecordEntry) == false)
        {
            AddWarning($"Could not retrieve database record for '{DatabaseName}' database. Skipping it.");
            return false;
        }
        
        DatabaseInfo = new DatabaseAnalysisInfo
        {
            DatabaseRecordEntry = databaseRecordEntry,
            DatabaseRecord = databaseRecordEntry.Deserialize<DatabaseRecord>()
        };

        if (DatabaseInfo.DatabaseRecord.Disabled)
        {
            return false;
        }
        
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

        DatabaseInfo.Stats = stats;
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

            if (compression != null)
            {
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
    }
}
