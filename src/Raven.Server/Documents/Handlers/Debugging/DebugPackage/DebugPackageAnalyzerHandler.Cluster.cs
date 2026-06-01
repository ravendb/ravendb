using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public partial class DebugPackageAnalyzerHandler : ServerRequestHandler
{
    [RavenAction("/debug/info-package/analyzer/cluster/topology", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetClusterTopology()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetStringQueryString("nodeTag");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        DebugPackageNodeReport nodeReport;

        if (string.IsNullOrEmpty(nodeTag))
            nodeReport = packageReport.Reports.FirstOrDefault(x => x.ClusterNode?.NodeStateInfo?.TopologyEntry != null);
        else
            nodeReport = packageReport.ForNode(nodeTag);

        await WriteEntryOrNotFoundAsync(nodeReport?.ClusterNode?.NodeStateInfo?.TopologyEntry);
    }
    
    
    [RavenAction("/debug/info-package/analyzer/cluster/log", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetLogs()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);

        await WriteEntryOrNotFoundAsync(nodeReport.ClusterNode?.NodeLogInfo?.LogEntry);
    }
    
    [RavenAction("/debug/info-package/analyzer/cluster/observer/decisions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetObserverDecisions()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.Reports.FirstOrDefault(x => x.ClusterNode?.ObserverInfo != null);

        await WriteEntryOrNotFoundAsync(nodeReport?.ClusterNode?.ObserverInfo?.ObserverDecisionsEntry);
    }
}
