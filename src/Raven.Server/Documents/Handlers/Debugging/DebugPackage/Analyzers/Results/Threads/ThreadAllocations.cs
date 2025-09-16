using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Threads;

public class ThreadAllocations : IDynamicJson
{
    public string ThreadName { get; set; }
    public long Allocations { get; set; }
    public string HumaneAllocations { get; set; }
    public List<ThreadId> ThreadIds { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ThreadName)] = ThreadName,
            [nameof(Allocations)] = Allocations,
            [nameof(HumaneAllocations)] = HumaneAllocations,
            [nameof(ThreadIds)] = new DynamicJsonArray(ThreadIds.Select(x => x.ToJson()))
        };
    }
}
