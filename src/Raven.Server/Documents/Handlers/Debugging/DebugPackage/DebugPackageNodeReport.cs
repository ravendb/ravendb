using System.Linq;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageNodeReport(string nodeTag)
{
    public string NodeTag { get; } = nodeTag;

    public DebugPackageAnalysisIssues DetectedIssues  {  get; set; }
    
    public DebugPackageAnalyzeErrors AnalyzeErrors { get; set; }
    
    public MachineAnalysisInfo Machine { get; set; }
    
    public ServerAnalysisInfo Server { get; set; }
    
    public ClusterAnalysisInfo ClusterNode { get; set; }
    
    public DebugPackageDatabaseReport[] Databases { get; set; }

    public DebugPackageDatabaseReport ForDatabase(string databaseName)
    {
        return Databases.SingleOrDefault(x => x.DatabaseName == databaseName);
    }
}
