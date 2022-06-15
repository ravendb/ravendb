using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Maintenance.Sharding
{
    public static class BucketsMigrator
    {
        public delegate bool MoveStrategy(DatabaseRecord record, ShardReport[] shards, MigrationPolicy policy, ref ShardMigrationResult result);

        public static bool NeedBalanceForDatabase(DatabaseRecord record, ShardReport[] shards, MigrationPolicy policy, MoveStrategy moveStrategy, out ShardMigrationResult result)
        {
            result = null;

            if (shards.Length < 1)
                return false;

            if (record.Sharding.ShardBucketMigrations.Any(b => b.Value.Status < MigrationStatus.OwnershipTransferred))
                return false; // other migration is ongoing

            return moveStrategy(record, shards, policy, ref result);
        }

        public static bool EdgeMove(DatabaseRecord record, ShardReport[] shards, MigrationPolicy policy, ref ShardMigrationResult result)
        {
            ShardReport smallest = shards[0];
            ShardReport biggest = smallest;

            for (int i = 1; i < shards.Length; i++)
            {
                var shard = shards[i];

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
                for (var index = 0; index < record.Sharding.ShardBucketRanges.Count; index++)
                {
                    var range = record.Sharding.ShardBucketRanges[index];
                    if (range.ShardNumber != biggest.Shard)
                        continue;

                    // lower
                    if (index != 0)
                        CheckBucket(range.BucketRangeStart, record.Sharding.ShardBucketRanges[index - 1].ShardNumber);

                    // upper
                    if (index != record.Sharding.ShardBucketRanges.Count - 1)
                    {
                        var next = record.Sharding.ShardBucketRanges[index + 1];
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
        public static bool NaiveMove(DatabaseRecord record, ShardReport[] shards, MigrationPolicy policy, ref ShardMigrationResult result)
        {
            ShardReport smallest = shards[0];
            ShardReport biggest = smallest;

            for (int i = 1; i < shards.Length; i++)
            {
                var shard = shards[i];

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
                    if (ShardHelper.GetShardNumber(record.Sharding.ShardBucketRanges, b.Key) != biggest.Shard)
                        return long.MaxValue;

                    if (record.Sharding.ShardBucketMigrations.ContainsKey(b.Key))
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

        public static Dictionary<string, MergedDatabaseStatusReport> BuildMergedReports(Dictionary<string, ClusterNodeStatusReport> current, Dictionary<string, ClusterNodeStatusReport> previous)
        {
            var mergedReport = new Dictionary<string, MergedDatabaseStatusReport>();

            PopulateReport(current, mergedReport);
            PopulateReport(previous, mergedReport);

            return mergedReport;
        }

        private static void PopulateReport(Dictionary<string, ClusterNodeStatusReport> clusterReport, Dictionary<string, MergedDatabaseStatusReport> mergedReport)
        {
            foreach (var node in clusterReport)
            foreach (var database in node.Value.Report)
            {
                var fullName = database.Key;
                var shardSeparator = fullName.IndexOf('$');
                if (shardSeparator < 0)
                    continue;

                var split = fullName.Split('$');
                var name = split[0];
                var shardNumber = int.Parse(split[1]);

                mergedReport.TryAdd(name, new MergedDatabaseStatusReport());
                if (mergedReport[name].MergedReport.TryGetValue(shardNumber, out var currentReport) == false)
                {
                    mergedReport[name].MergedReport.Add(shardNumber, database.Value);
                }
                else
                {
                    var conflict = ChangeVectorUtils.GetConflictStatus(database.Value.DatabaseChangeVector, currentReport.DatabaseChangeVector);
                    switch (conflict)
                    {
                        case ConflictStatus.Update:
                            mergedReport[name].MergedReport[shardNumber] = database.Value;
                            break;
                        case ConflictStatus.Conflict:
                            var distance = ChangeVectorUtils.Distance(database.Value.DatabaseChangeVector, currentReport.DatabaseChangeVector);
                            if (distance > 0)
                                mergedReport[name].MergedReport[shardNumber] = database.Value;
                            break;
                        case ConflictStatus.AlreadyMerged:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }
    }

    public class ShardReport
    {
        public ShardNumber Shard;
        public Dictionary<BucketNumber, BucketReport> ReportPerBucket;
        public long TotalSize => ReportPerBucket.Sum(r => r.Value.Size);
    }

    public class BucketReport
    {
        public long Size;
        public long NumberOfDocuments;
        public DateTime LastAccess;
    }

    public class MigrationPolicy
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

    public struct BucketNumber
    {
        private int _value = 0;

        public BucketNumber(int value)
        {
            _value = value;
        }

        public static implicit operator BucketNumber(int value) => new BucketNumber(value: value);

        public static implicit operator int(BucketNumber value) => value._value;

        public override string ToString() => _value.ToString();
    }
}
