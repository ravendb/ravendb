using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class MemoryAnalysisBasicInfo : IDynamicJson
{
    public string PhysicalMemory { get; set; }
    public string WorkingSet { get; set; }
    public string ManagedAllocations { get; set; }
    public string UnmanagedAllocations { get; set; }
    public string AvailableMemory { get; set; }
    public string AvailableMemoryForProcessing { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PhysicalMemory)] = PhysicalMemory,
            [nameof(WorkingSet)] = WorkingSet,
            [nameof(ManagedAllocations)] = ManagedAllocations,
            [nameof(UnmanagedAllocations)] = UnmanagedAllocations,
            [nameof(AvailableMemory)] = AvailableMemory,
            [nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing
        };
    }
}
