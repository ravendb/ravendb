using System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

public class ClusterAlert
{
    public string NodeTag { get; set; }
    public string Title { get; set; }
    public string Message { get; set; }
    public string Exception { get; set; }
    public DateTime? CreatedAt { get; set; }
}
