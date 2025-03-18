using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations;

public class CollectionRevisionsStatistics
{
    public long CountOfRevisions { get; set; }
    public Dictionary<string, long> Collections { get; set; }

    public DynamicJsonValue ToJson()
    {
        DynamicJsonValue collections = new DynamicJsonValue();
        foreach (var collection in Collections)
        {
            collections[collection.Key] = collection.Value;
        }

        return new DynamicJsonValue()
        {
            [nameof(CountOfRevisions)] = CountOfRevisions,
            [nameof(Collections)] = collections
        };
    }
}
