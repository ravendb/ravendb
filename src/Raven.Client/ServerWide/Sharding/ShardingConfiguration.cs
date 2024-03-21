using System.Collections.Generic;

namespace Raven.Client.ServerWide.Sharding;

public sealed class ShardingConfiguration
{
    public OrchestratorConfiguration Orchestrator;

    public Dictionary<int, DatabaseTopology> Shards;

    public List<ShardBucketRange> BucketRanges = new List<ShardBucketRange>();

    public List<PrefixedShardingSetting> Prefixed;

    public Dictionary<int, ShardBucketMigration> BucketMigrations;

    // change vectors with a MOVE element below this will be considered as permanent
    // pointers with the migration index below this one will be purged
    public long MigrationCutOffIndex;

    // the dbid part with the MOVE tag upon migration
    public string DatabaseId;

    internal bool DoesShardHaveBuckets(int shardNumber) => DoesShardHaveBuckets(BucketRanges, shardNumber);

    internal static bool DoesShardHaveBuckets(List<ShardBucketRange> bucketRanges, int shardNumber)
    {
        foreach (var bucketRange in bucketRanges)
        {
            if (bucketRange.ShardNumber == shardNumber)
                return true;
        }

        return false;
    }

    internal bool HasActiveMigrations()
    {
        foreach (var m in BucketMigrations)
        {
            if (m.Value.IsActive)
                return true;
        }

        return false;
    }

    internal bool DoesShardHavePrefixes(int shardNumber) => DoesShardHavePrefixes(Prefixed, shardNumber);

    internal static bool DoesShardHavePrefixes(List<PrefixedShardingSetting> prefixes, int shardNumber)
    {
        foreach (var setting in prefixes)
        {
            if (setting.Shards.Contains(shardNumber))
                return true;
        }

        return false;
    }
}
