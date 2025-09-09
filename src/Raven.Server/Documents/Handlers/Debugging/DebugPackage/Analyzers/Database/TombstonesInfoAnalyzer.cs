using System.Collections.Generic;
using Raven.Server.Documents.Commands.Tombstones;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class TombstonesInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private GetTombstonesStateCommand.Response TombstonesState { get; set; }
    
    private List<TombstoneCleaner.TombstonesState.SubscriptionInfoExtended> TombstonesStateExtended { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetValue<AdminTombstoneHandler, GetTombstonesStateCommand.Response>(x => x.State(), out var tombstonesState) == false)
        {
            AddWarning("Failed to get tombstones state");
        }

        if (entries.TryGetValue<AdminTombstoneHandler, List<TombstoneCleaner.TombstonesState.SubscriptionInfoExtended>>(
                x => x.State(), nameof(TombstoneCleaner.TombstonesState.PerSubscriptionInfoExtended),
                out var tombstonesStateExtended) == false)
        {
            AddWarning("Failed to get tombstones state extended");
            return false;
        }
        TombstonesState = tombstonesState;
        TombstonesStateExtended = tombstonesStateExtended;
        return true;
    }


    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        foreach (var extendedInfo in TombstonesStateExtended)
        {
            if (extendedInfo.NumberOfTombstoneLeft > 1000)
            {
                string tombstonesTypes = string.Empty;
                
                if (extendedInfo.Types.Documents > 0)
                    tombstonesTypes += "document ";
                if (extendedInfo.Types.Counters > 0)
                    tombstonesTypes += "counter ";
                if (extendedInfo.Types.TimeSeries > 0)
                    tombstonesTypes += "time series ";
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    $"Blocking {tombstonesTypes}tombstones by '{extendedInfo.Identifier}' {extendedInfo.Process}",
                    $"There are {extendedInfo.NumberOfTombstoneLeft} tombstones that are blocked by the given process from being deleted. " +
                    "Please check the state of the process (is it disabled maybe?).",
                    IssueSeverity.Warning,
                    IssueCategory.Database));
            }
        }
    }
}
