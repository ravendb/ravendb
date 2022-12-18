using System.Collections.Generic;

namespace Raven.Client.ServerWide.Sharding;

public class PrefixedShardingSetting
{
    public List<int> Shards { get; set; }
}
