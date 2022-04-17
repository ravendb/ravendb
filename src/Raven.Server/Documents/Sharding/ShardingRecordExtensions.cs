using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding;

public static class ShardingRecordExtensions
{
    public static List<ShardBucketRange> For(this ShardingRecord record, string id)
    {
        if (record.Prefixed == null)
            return record.ShardBucketRanges;

        foreach (var kvp in record.Prefixed)
        {
            if (id.StartsWith(kvp.Key, StringComparison.OrdinalIgnoreCase))
                return kvp.Value;
        }

        return record.ShardBucketRanges;
    }

    public static List<ShardBucketRange> For(this ShardingRecord record, Slice id)
    {
        if (record.Prefixed == null)
            return record.ShardBucketRanges;

        var idAsStr = id.ToString();        //TODO: optimize this to avoid those allocations
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Need to optimize this allocation");
        foreach (var kvp in record.Prefixed)
        {
            if (idAsStr.StartsWith(kvp.Key, StringComparison.OrdinalIgnoreCase))
                return kvp.Value;
        }

        return record.ShardBucketRanges;
    }
}
