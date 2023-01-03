using System.Collections.Generic;

namespace Raven.Client.ServerWide.Sharding;

public class ShardingConfiguration
{
    public OrchestratorConfiguration Orchestrator;

    public Dictionary<int, DatabaseTopology> Shards;

    public List<ShardBucketRange> BucketRanges = new List<ShardBucketRange>();

    public Dictionary<string, List<ShardBucketRange>> Prefixed;

    public Dictionary<int, ShardBucketMigration> BucketMigrations;

    // change vectors with a MOVE element below this will be considered as permanent
    // pointers with the migration index below this one will be purged
    public long MigrationCutOffIndex;

    // the dbid part with the MOVE tag upon migration
    public string DatabaseId;

    public bool DoesShardHaveBuckets(int shardNumber) => DoesShardHaveBuckets(BucketRanges, shardNumber);

    public static bool DoesShardHaveBuckets(List<ShardBucketRange> bucketRanges, int shardNumber)
    {
        foreach (var bucketRange in bucketRanges)
        {
            if (bucketRange.ShardNumber == shardNumber)
                return true;
        }

        return false;
    }
}
