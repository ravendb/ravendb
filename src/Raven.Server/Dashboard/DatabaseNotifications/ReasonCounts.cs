using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.DatabaseNotifications;

public class ReasonCounts
{
    public Dictionary<string, long> ByReason { get; } = new();
    public long TotalCount { get; set; }

    public void Increment(string reason)
    {
        if (ByReason.TryGetValue(reason, out var count) == false)
            ByReason[reason] = 1;
        else
            ByReason[reason] = count + 1;

        TotalCount++;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue()
        {
            [nameof(TotalCount)] = TotalCount
        };
        
        foreach (var kvp in ByReason)
        {
            json[kvp.Key] = kvp.Value;
        }
        
        return json;
    }
}
