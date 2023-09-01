using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    public static class BucketsMigrator
    {
        public delegate bool MoveStrategy(DatabaseRecord record, Dictionary<int, ShardReport> shards, MigrationPolicy policy, ref ShardMigrationResult result);

        public static bool NeedBalanceForDatabase(DatabaseRecord record, Dictionary<int, ShardReport> shards, MigrationPolicy policy, MoveStrategy moveStrategy, out ShardMigrationResult result)
        {
            result = null;

            if (shards.Count < 1)
                return false;

            if (record.Sharding.BucketMigrations.Any(b => b.Value.IsActive))
                return false; // other migration is ongoing

            return moveStrategy(record, shards, policy, ref result);
        }

        public static bool EdgeMove(DatabaseRecord record, Dictionary<int, ShardReport> shards, MigrationPolicy policy, ref ShardMigrationResult result)
        {
            ShardReport smallest = shards[0];
            ShardReport biggest = smallest;

            foreach (var shard in shards.Values)
            {
                if (shard.TotalSize > biggest.TotalSize)
                    biggest = shard;

                if (shard.TotalSize < smallest.TotalSize)
                    smallest = shard;
            }

            if (biggest.TotalSize - smallest.TotalSize > policy.SizeThreshold && 
                smallest.TotalSize/biggest.TotalSize < 1 - policy.PercentageThreshold)
            {
                var bucketToMove = -1;
                var toShard = -1;
                var minSize = long.MaxValue;

                // smallest edge bucket in the biggest shard
                for (var index = 0; index < record.Sharding.BucketRanges.Count; index++)
                {
                    var range = record.Sharding.BucketRanges[index];
                    if (range.ShardNumber != biggest.Shard)
                        continue;

                    // lower
                    if (index != 0)
                        CheckBucket(range.BucketRangeStart, record.Sharding.BucketRanges[index - 1].ShardNumber);

                    // upper
                    if (index != record.Sharding.BucketRanges.Count - 1)
                    {
                        var next = record.Sharding.BucketRanges[index + 1];
                        CheckBucket(next.BucketRangeStart - 1, next.ShardNumber);
                    }
                }

                result = new ShardMigrationResult
                {
                    Database = record.DatabaseName, 
                    Bucket = bucketToMove, 
                    DestinationShard = toShard, 
                    SourceShard = biggest.Shard
                };
                return true;

                void CheckBucket(int bucket, int neighborShard)
                {
                    var size = biggest.ReportPerBucket[bucket].Size;
                    if (size < minSize)
                    {
                        minSize = size;
                        bucketToMove = bucket;
                        toShard = neighborShard;
                    }
                }
            }

            return false;
        }

        // Move the smallest bucket from the biggest shard to the smallest shard
        public static bool NaiveMove(DatabaseRecord record, Dictionary<int, ShardReport> shards, MigrationPolicy policy, ref ShardMigrationResult result)
        {
            ShardReport smallest = shards.Values.First();
            ShardReport biggest = smallest;

            foreach (var shard in shards.Values)
            {
                if (shard.TotalSize > biggest.TotalSize)
                    biggest = shard;

                if (shard.TotalSize < smallest.TotalSize)
                    smallest = shard;
            }

            if (biggest.TotalSize - smallest.TotalSize > policy.SizeThreshold)
            {
                // smallest bucket in the biggest shard
                var bucketToMove = biggest.ReportPerBucket.MinBy(b =>
                {
                    // ensure this bucket belongs to this shard
                    if (ShardHelper.GetShardNumberFor(record.Sharding, b.Key) != biggest.Shard)
                        return long.MaxValue;

                    if (record.Sharding.BucketMigrations.ContainsKey(b.Key))
                        return long.MaxValue;

                    return b.Value.Size;
                }).Key;

                result = new ShardMigrationResult
                {
                    Database = record.DatabaseName, 
                    Bucket = bucketToMove, 
                    DestinationShard = smallest.Shard, 
                    SourceShard = biggest.Shard
                };
                return true;
            }

            return false;
        }
    }

    public sealed class ShardReport
    {
        public ShardNumber Shard;
        public Dictionary<int, BucketReport> ReportPerBucket;
        public long TotalSize => ReportPerBucket.Sum(r => r.Value.Size);
    }

    public sealed class BucketReport : IDynamicJson
    {
        public long Size;
        public long NumberOfDocuments;
        public DateTime LastAccess;
        public string LastChangeVector;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Size)] = Size,
                [nameof(NumberOfDocuments)] = NumberOfDocuments,
                [nameof(LastAccess)] = LastAccess,
                [nameof(LastChangeVector)] = LastChangeVector,
            };
        }
    }

    public sealed class MigrationPolicy
    {
        public static MigrationPolicy Default = new MigrationPolicy();
        public static MigrationPolicy Min = new MigrationPolicy
        {
            SizeThreshold = 1L,
            PercentageThreshold = double.Epsilon
        };


        public double? PercentageThreshold = 0.05;
        public long? SizeThreshold = 10 * 1024 * 1024; // 10 MB 
    }
    
    public struct ShardNumber
    {
        private int _value = 0;

        public ShardNumber(int value)
        {
            _value = value;
        }

        public static implicit operator ShardNumber(int value) => new ShardNumber(value: value);

        public static implicit operator int(ShardNumber value) => value._value;
        public override string ToString() => _value.ToString();

    }
}
