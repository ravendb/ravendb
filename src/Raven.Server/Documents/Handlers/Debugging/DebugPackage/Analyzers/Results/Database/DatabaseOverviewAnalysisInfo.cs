using System.Collections.Generic;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class DatabaseOverviewAnalysisInfo : IDynamicJson
{
    public string DatabaseName { get; set; }
    public List<DetectedIssue> Issues { get; set; }
    public List<DetectedIssue> DatabaseGroupIssues { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        throw new System.NotImplementedException();
    }
}
