using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class DatabaseAnalysisInfo
{
    public DatabaseStatistics Stats { get; set; }
    public DatabaseRecord DatabaseRecord { get; set; }
    public DebugPackageEntries.Entry DatabaseRecordEntry { get; set; }
}
