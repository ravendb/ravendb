using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Sharding;

public class PrefixedShardingSetting
{
    public string Prefix { get; set; }

    public List<int> Shards { get; set; }

    [ForceJsonSerialization]
    internal int BucketRangeStart { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Prefix)] = Prefix, 
            [nameof(Shards)] = new DynamicJsonArray(Shards), 
            [nameof(BucketRangeStart)] = BucketRangeStart
        };
    }
}
