using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public partial class DebugPackageAnalyzerHandler : ServerRequestHandler
{
    [RavenAction("/debug/info-package/analyzer/network", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetNetworkInfo()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            context.Write(writer, nodeReport.Server.NetworkInfo.ToJson());
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/memory", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetMemoryInfo()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");

        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            context.Write(writer, nodeReport.Server.MemoryInfo.ToJson());
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/storage-io", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetStorageIoInfo()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");

        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        using (var json = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(nodeReport.Server.StorageIo ?? new StorageIoAnalysisInfo(), context))
        {
            writer.WriteObject(json);
        }
    }

    [RavenAction("/debug/info-package/analyzer/threads/runaway", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task RunawayThreads()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            context.Write(writer, nodeReport.Server.ThreadsInfo.Threads.ToJson());
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/threads/stack-trace", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task StackTrace()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;

        var nodeReport = packageReport.ForNode(nodeTag);

        await WriteEntryOrNotFoundAsync(nodeReport.Server?.ThreadsInfo?.StackTracesEntry);
    }
}
