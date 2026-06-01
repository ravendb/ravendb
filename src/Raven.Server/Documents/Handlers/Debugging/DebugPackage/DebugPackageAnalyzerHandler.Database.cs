using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using NuGet.Protocol;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public partial class DebugPackageAnalyzerHandler : ServerRequestHandler
{
    [RavenAction("/debug/info-package/analyzer/databases/overview", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetOverview()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetReportOrSetNotFound(packageId, out var packageReport) == false)
            return;
        
        var nodeReport = packageReport.ForNode(nodeTag);
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            var databaseOverviewAnalysisInfo = new DatabaseOverviewAnalysisInfo
            {
                DatabaseName = dbName,
            };
            
            if (nodeReport.DetectedIssues.DatabaseIssues.TryGetValue(dbName, out var issues))
                databaseOverviewAnalysisInfo.Issues = issues;
            
            if (packageReport.ClusterWideIssues.DatabaseIssues.TryGetValue(dbName, out var databaseGroupIssues))
                databaseOverviewAnalysisInfo.DatabaseGroupIssues = databaseGroupIssues;
            
            context.Write(writer, databaseOverviewAnalysisInfo.ToJson());
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetStats()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            writer.WriteDatabaseStatistics(context, dbReport.DatabaseInfo.Stats);
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/record", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetRecord()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        var responseStream = ResponseBodyStream();

        await dbReport.DatabaseInfo.DatabaseRecordEntry.WriteContentToAsync(responseStream);
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/indexes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetIndexes()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        var responseStream = ResponseBodyStream();

        await dbReport.IndexesInfo.DefinitionsEntry.WriteContentToAsync(responseStream);
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/indexes/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetIndexesStats()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            writer.WriteIndexesStats(context, dbReport.IndexesInfo.Stats);
        }
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/indexes/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetIndexingPerformance()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        var responseStream = ResponseBodyStream();

        await dbReport.IndexesInfo.PerformanceEntry.WriteContentToAsync(responseStream);
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/indexes/errors", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetIndexingErrors()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        var responseStream = ResponseBodyStream();

        await dbReport.IndexesInfo.ErrorsEntry.WriteContentToAsync(responseStream);
    }
    
    [RavenAction("/debug/info-package/analyzer/databases/configuration/settings", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetSettings()
    {
        var packageId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("packageId");
        var nodeTag = GetQueryStringValueAndAssertIfSingleAndNotEmpty("nodeTag");
        var dbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
        
        if (TryGetDatabaseReportOrSetNotFound(packageId, nodeTag, dbName, out var dbReport) == false)
            return;
        
        var responseStream = ResponseBodyStream();

        await dbReport.Settings.SettingsEntry.WriteContentToAsync(responseStream);
    }
    
    private bool TryGetDatabaseReportOrSetNotFound(string packageId, string nodeTag, string dbName, out DebugPackageDatabaseReport dbReport)
    {
        if (DebugPackageReportsContainer.TryGet(packageId, out var report) == false)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            dbReport = null;
            return false;
        }
        
        var nodeReport = report.ForNode(nodeTag);

        dbReport = nodeReport.Databases.FirstOrDefault(x => x.DatabaseName == dbName);

        if (dbReport == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return false;
        }
        
        return true;
    }
}
