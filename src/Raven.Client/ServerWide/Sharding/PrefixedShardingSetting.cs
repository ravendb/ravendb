using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding;

public class PrefixedShardingSetting
{
    public string Prefix { get; set; }

    public List<int> Shards { get; set; }

    [ForceJsonSerialization]
    internal int BucketRangeStart { get; set; }
}
