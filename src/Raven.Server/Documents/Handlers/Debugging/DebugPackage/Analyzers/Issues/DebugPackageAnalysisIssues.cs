using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

public class DebugPackageAnalysisIssues : IDynamicJson
{
    public List<DetectedIssue> ServerIssues { get; set; } = [];
    
    public List<DetectedIssue> ClusterIssues { get; set; } = [];

    public Dictionary<string, List<DetectedIssue>> DatabaseIssues { get; set; } = [];
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ServerIssues)] = new DynamicJsonArray(ServerIssues.Select(issue => issue.ToJson())),
            [nameof(ClusterIssues)] = new DynamicJsonArray(ClusterIssues.Select(issue => issue.ToJson())),
            [nameof(DatabaseIssues)] = DynamicJsonValue.Convert(DatabaseIssues),
        };
    }

    public List<DetectedIssue> ForDatabase(string databaseName)
    {
        if (DatabaseIssues.TryGetValue(databaseName, out var issues))
            return issues;
    
        var newIssues = new List<DetectedIssue>();
        DatabaseIssues[databaseName] = newIssues;
        return newIssues;
    }
}
