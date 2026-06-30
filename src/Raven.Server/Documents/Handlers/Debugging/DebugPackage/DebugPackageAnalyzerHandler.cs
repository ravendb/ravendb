using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public partial class DebugPackageAnalyzerHandler : ServerRequestHandler
{
    [RavenAction("/debug/info-package/analyzer/upload", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task UploadDebugPackage()
    {
        using (var zipMemoryStream = new MemoryStream())
        {
            await HttpContext.Request.Body.CopyToAsync(zipMemoryStream);
            zipMemoryStream.Position = 0;

            var analyzer = new DebugPackageAnalyzer(zipMemoryStream);

            var report = analyzer.Analyze();

            if (DebugPackageReportsContainer.TryAdd(report.PackageId, report) == false)
                throw new IOException("Failed to add Debug Package report");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, report.GetSummary().ToJson());
            }
        }
    }

    [RavenAction("/debug/info-package/analyzer/remove", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public Task RemoveDebugPackageAnalysis()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        
        if (DebugPackageReportsContainer.TryRemove(packageId) == false)
            throw new IOException("Failed to remove Debug Package report");

        return NoContent();
    }

    [RavenAction("/debug/info-package/analyzer/summary", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetSummary()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");

        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            context.Write(writer, packageReport.GetSummary().ToJson());
        }
    }

    [RavenAction("/debug/info-package/analyzer/summary/node", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetNodeSummary()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");

        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;
        
        var debugPackageAnalysisSummary = packageReport.GetSummary();

        if (debugPackageAnalysisSummary.SummaryPerNode.TryGetValue(nodeTag, out var nodeSummary) == false)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            context.Write(writer, nodeSummary.ToJson());
        }
    }

    private bool TryGetReportOrSetNotFound(string packageId, out DebugPackageReport report)
    {
        if (DebugPackageReportsContainer.TryGet(packageId, out report) == false)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return false;
        }

        return true;
    }

    private async Task WriteEntryOrNotFoundAsync(DebugPackageEntries.Entry entry)
    {
        if (entry == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        await entry.WriteContentToAsync(ResponseBodyStream());
    }
}
