using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Threads;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

public class UnmanagedMemoryAnalysisInfo : IDynamicJson
{
    public string UnmanagedAllocations { get; set; }
    public string LuceneUnmanagedAllocationsForTermCache { get; set; }
    public string LuceneUnmanagedAllocationsForSorting { get; set; }
    public string EncryptionBuffersInUse { get; set; }
    public string EncryptionBuffersPool { get; set; }
    public string EncryptionLockedMemory { get; set; }
    public List<ThreadAllocations> ThreadAllocations { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(UnmanagedAllocations)] = UnmanagedAllocations,
            [nameof(LuceneUnmanagedAllocationsForTermCache)] = LuceneUnmanagedAllocationsForTermCache,
            [nameof(LuceneUnmanagedAllocationsForSorting)] = LuceneUnmanagedAllocationsForSorting,
            [nameof(EncryptionBuffersInUse)] = EncryptionBuffersInUse,
            [nameof(EncryptionBuffersPool)] = EncryptionBuffersPool,
            [nameof(EncryptionLockedMemory)] = EncryptionLockedMemory,
            [nameof(ThreadAllocations)] = ThreadAllocations != null 
                ? new DynamicJsonArray(ThreadAllocations.Select(x => x.ToJson()))
                : null
        };
    }
}
