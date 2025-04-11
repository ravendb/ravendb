using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Threads;

public class ThreadId : IDynamicJson
{
    public long Id { get; set; }
    
    public long ManagedThreadId { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(ManagedThreadId)] = ManagedThreadId
        };
    }
}
