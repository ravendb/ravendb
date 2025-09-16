using Raven.Server.Dashboard;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class ThreadsAnalysisInfo
{
    public ThreadsInfo Threads { get; set; }
    public DebugPackageEntries.Entry StackTracesEntry { get; set; }
}
