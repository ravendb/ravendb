using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Corax.Querying.Planning;

public sealed class PlanDecisionTrail : IDynamicJson
{
    public List<PlanDecisionEntry> Entries { get; } = [];

    public void Record(string optimization, bool accepted, string reason)
    {
        Entries.Add(new PlanDecisionEntry(optimization, accepted, reason));
    }

    public DynamicJsonValue ToJson()
    {
        var arr = new DynamicJsonArray();
        foreach (var entry in Entries)
            arr.Add(entry.ToJson());
        return new DynamicJsonValue { [nameof(Entries)] = arr };
    }
}

public record PlanDecisionEntry(string Optimization, bool Accepted, string Reason)
{
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Optimization)] = Optimization,
            [nameof(Accepted)] = Accepted,
            [nameof(Reason)] = Reason
        };
    }
}
