using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class ManagedMemoryAnalysisInfo : IDynamicJson
{
    public string ManagedAllocations { get; set; }

    public string LuceneManagedAllocationsForTermCache { get; set; }

    public GcRunInfo LastGcInfo { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ManagedAllocations)] = ManagedAllocations,
            [nameof(LuceneManagedAllocationsForTermCache)] = LuceneManagedAllocationsForTermCache,
            [nameof(LastGcInfo)] = LastGcInfo?.ToJson()
        };
    }
}
