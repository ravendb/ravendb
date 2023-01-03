using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Sharding;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding;

public static class ShardingConfigurationExtensions
{
    public static List<ShardBucketRange> For(this ShardingConfiguration configuration, string id)
    {
        return GetRanges(configuration.BucketRanges, configuration.Prefixed, id);
    }

    public static List<ShardBucketRange> For(this RawShardingConfiguration configuration, string id)
    {
        return GetRanges(configuration.BucketRanges, configuration.Prefixed, id);
    }

    public static List<ShardBucketRange> For(this ShardingConfiguration configuration, Slice id)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Need to optimize this allocation");

        return GetRanges(configuration.BucketRanges, configuration.Prefixed, id.ToString());
    }

    public static List<ShardBucketRange> For(this RawShardingConfiguration configuration, Slice id)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Need to optimize this allocation");

        return GetRanges(configuration.BucketRanges, configuration.Prefixed, id.ToString());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static List<ShardBucketRange> GetRanges(List<ShardBucketRange> bucketRanges, Dictionary<string, List<ShardBucketRange>> prefixed, string id)
    {
        if (prefixed == null || prefixed.Count == 0)
            return bucketRanges;

        foreach (var kvp in prefixed)
        {
            if (id.StartsWith(kvp.Key, StringComparison.OrdinalIgnoreCase))
                return kvp.Value;
        }

        return bucketRanges;
    }
}
