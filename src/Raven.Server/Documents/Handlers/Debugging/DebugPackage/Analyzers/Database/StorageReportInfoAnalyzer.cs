using System.IO;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Voron.Debugging;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class StorageReportInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    public EnvironmentStorageReport StorageReport { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetValue<StorageHandler, EnvironmentStorageReport>(x => x.Report(), out var storageReport) == false)
        {
            AddWarning("Failed to get storage report");
        }
        
        StorageReport = storageReport;

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        foreach (var item in StorageReport.Results)
        {
            var report = item.Report;

            long dataFileSizeInBytes = report.DataFile.AllocatedSpaceInBytes;
            if (dataFileSizeInBytes < Size.GB / 2)
            {
                // skip small storages
                continue;
            }
            
            double freeSpacePercentage = 1.0 * report.DataFile.FreeSpaceInBytes / dataFileSizeInBytes;

            var freeSpaceThreshold = dataFileSizeInBytes > 8 * Size.GB ? 0.5 : 0.6;
            
            if (freeSpacePercentage > freeSpaceThreshold)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    $"High free space detected in {item.Type.ToLower()} storage ({item.Name})",
                    $"Data file '{Path.Combine(StorageReport.BasePath, item.Name, "Raven.voron")}' has {freeSpacePercentage:P} free space",
                    IssueSeverity.Info,
                    IssueCategory.Database)
                {
                    RecommendedAction  = "Consider compacting this storage to reduce its size if you need to free up space"
                });
            }

            var allTempFilesSize = 0L;

            foreach (var tempBufferReport in report.TempFiles)
            {
                allTempFilesSize += tempBufferReport.AllocatedSpaceInBytes;
            }
            
            var tempSpaceThreshold = dataFileSizeInBytes > 8 * Size.GB ? 0.5 : 0.6;
            
            if (allTempFilesSize > dataFileSizeInBytes * tempSpaceThreshold)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    $"High temporary files size detected in {item.Type.ToLower()} storage ({item.Name})",
                    $"Temp files size is {new Size(allTempFilesSize).HumaneSize} while the size of the data file ({Path.Combine(StorageReport.BasePath, item.Name, "Raven.voron")}) is " +
                    $"{new Size(dataFileSizeInBytes).HumaneSize}",
                    IssueSeverity.Info,
                    IssueCategory.Database)
                );
            }
        }
    }
}
