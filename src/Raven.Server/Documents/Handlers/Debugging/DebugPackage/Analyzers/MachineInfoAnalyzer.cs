using System.Collections.Generic;
using System.Linq;
using Raven.Client.Http;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class MachineInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    public MachineInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
    }
    
    public MachineAnalysisInfo MachineInfo { get; } = new();
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetValue<RachisAdminHandler, Dictionary<string, DetailsPerNode>>(x => x.GetClusterTopology(),
                nameof(LicenseLimits.NodeLicenseDetails), out var osDetailsPerNode) == false)
        {
            AddWarning("Could not retrieve machine details of cluster nodes");
            return false;
        }

        if (serverEntries.TryGetValue<RachisAdminHandler, string>(x => x.GetClusterTopology(), 
                nameof(ClusterTopologyResponse.NodeTag), out var nodeTag) == false)
        {
            AddWarning("Could not retrieve node tag");
            return false;
        }

        var machineDetails = osDetailsPerNode.FirstOrDefault(x => x.Key == nodeTag).Value;

        if (machineDetails == null)
        {
            AddError($"Could not find machine details for current node - {nodeTag}");
            return false;
        }
        
        MachineInfo.NumberOfCores = machineDetails.NumberOfCores;
        MachineInfo.UtilizedCores = machineDetails.UtilizedCores;
        MachineInfo.InstalledMemoryInGb = machineDetails.InstalledMemoryInGb;
        MachineInfo.UsableMemoryInGb = machineDetails.UsableMemoryInGb;
        MachineInfo.OsInfo = machineDetails.OsInfo;
        
        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (MachineInfo.UtilizedCores < MachineInfo.NumberOfCores)
        {
            issues.ServerIssues.Add(
                new DetectedIssue("Underutilized cores",
                    $"Your machine has {MachineInfo.NumberOfCores} cores while RavenDB server utilizes only {MachineInfo.UtilizedCores}",
                    IssueSeverity.Info, IssueCategory.General));
        }
    }
}




