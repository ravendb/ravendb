using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class MemoryAnalysisInfo : IDynamicJson
{
    public string PhysicalMemory { get; set; }
    public string WorkingSet { get; set; }
    public string AvailableMemory { get; set; }
    public string AvailableMemoryForProcessing { get; set; }
    public ManagedMemoryAnalysisInfo Managed { get; set; } = new();
    public UnmanagedMemoryAnalysisInfo Unmanaged { get; set; } = new();
    public string MemoryMapped { get; set; }
    public bool IsHighDirty { get; set; }
    public string DirtyMemory { get; set; }
    
    public MemoryAnalysisBasicInfo GetBasicInfo()
    {
        return new MemoryAnalysisBasicInfo
        {
            PhysicalMemory = PhysicalMemory,
            WorkingSet = WorkingSet,
            ManagedAllocations = Managed.ManagedAllocations,
            UnmanagedAllocations = Unmanaged.UnmanagedAllocations,
            AvailableMemory = AvailableMemory,
            AvailableMemoryForProcessing = AvailableMemoryForProcessing,
        };
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PhysicalMemory)] = PhysicalMemory,
            [nameof(WorkingSet)] = WorkingSet,
            [nameof(AvailableMemory)] = AvailableMemory,
            [nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing,
            [nameof(Managed)] = Managed?.ToJson(),
            [nameof(Unmanaged)] = Unmanaged?.ToJson(),
            [nameof(MemoryMapped)] = MemoryMapped,
            [nameof(IsHighDirty)] = IsHighDirty,
            [nameof(DirtyMemory)] = DirtyMemory,
        };
    }
}
