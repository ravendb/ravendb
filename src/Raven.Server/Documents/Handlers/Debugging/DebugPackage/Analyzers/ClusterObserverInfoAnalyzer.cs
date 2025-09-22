using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;
using Raven.Server.ServerWide.Maintenance;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class ClusterObserverInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    public ClusterObserverInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
    }

    public ClusterObserverAnalysisInfo ObserverAnalysisInfo { get; private set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetEntry<RachisAdminHandler>(x => x.GetObserverDecisions(), out var observerDecisionsEntry) == false)
        {
            // cluster observer is available only on the leader node
            return false;
        }

        var observerDecisions = observerDecisionsEntry.Deserialize<ClusterObserverDecisions>();

        ObserverAnalysisInfo = new ClusterObserverAnalysisInfo
        {
            ObserverDecisionsEntry = observerDecisionsEntry, 
            ObserverDecisions = observerDecisions,
        };

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (ObserverAnalysisInfo.ObserverDecisions.Suspended)
        {
            issues.ClusterIssues.Add(new DetectedIssue("Cluster Observer is suspended",
                "Cluster Observer is suspended",
                IssueSeverity.Warning, IssueCategory.Cluster));
        }
    }
}
