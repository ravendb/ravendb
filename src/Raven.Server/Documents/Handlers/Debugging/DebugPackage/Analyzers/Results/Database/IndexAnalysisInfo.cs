using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class IndexesAnalysisInfo
{
    public IndexDefinition[] Definitions { get; set; }
    public IndexStats[] Stats { get; set; }
    public IndexMetadataInfo[] Metadata { get; set; }
    public IndexErrors[] Errors { get; set; }
    public DebugPackageEntries.Entry DefinitionsEntry { get; set; }
    public DebugPackageEntries.Entry PerformanceEntry { get; set; }
    public DebugPackageEntries.Entry ErrorsEntry { get; set; }
}
