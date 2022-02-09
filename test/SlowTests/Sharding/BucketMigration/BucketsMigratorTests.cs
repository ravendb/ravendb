using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.ServerWide.Maintenance.Sharding;
using Raven.Server.Utils;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.BucketMigration
{
    public class BucketsMigratorTests : NoDisposalNeeded
    {
        public BucketsMigratorTests(ITestOutputHelper output) : base(output)
        {
        }

        private List<DatabaseRecord.ShardRangeAssignment> PopulateRanges(int shards, int? seed = null)
        {
            var rnd = new Random(seed ?? 357);
            var list = new List<DatabaseRecord.ShardRangeAssignment>();
            var start = 0;
            while (true)
            {
                var range = new DatabaseRecord.ShardRangeAssignment();
                range.Shard = rnd.Next(0, shards);
                start += rnd.Next(0, 10 * 1024);
                range.RangeStart = start;
                if (start > ShardHelper.NumberOfBuckets)
                    return list;

                list.Add(range);
            }
        }

        private List<DatabaseRecord.ShardRangeAssignment> PopulateRangesEvenly(int shards)
        {
            var list = new List<DatabaseRecord.ShardRangeAssignment>();
            var start = 0;
            for (int i = 0; i < shards; i++)
            {
                var range = new DatabaseRecord.ShardRangeAssignment
                {
                    Shard = i,
                    RangeStart = start
                };
                list.Add(range);
                start += ShardHelper.NumberOfBuckets / shards;
            }

            return list;
        }

        private ShardReport[] CreateShardReports(DatabaseRecord record, int? seed = null)
        {
            var rnd = new Random(seed ?? 357);

            var shards = new ShardReport[record.Shards.Length];

            for (int bucket = 0; bucket < ShardHelper.NumberOfBuckets; bucket++)
            {
                var shardIndex = ShardHelper.GetShardIndex(record, bucket);
                shards[shardIndex] ??= new ShardReport
                {
                    Shard = shardIndex,
                    ReportPerBucket = new Dictionary<BucketNumber, BucketReport>()
                };

                shards[shardIndex].ReportPerBucket[bucket] = new BucketReport
                {
                    Size = (shardIndex + 1) * rnd.NextInt64(10*1024,1024*1024),
                    NumberOfDocuments = rnd.Next(10,1000)
                };
            }

            return shards;
        }

        [Fact(Skip = "Too stressful")]
        public void BalanceShardsWithNaiveApproach()
        {
            var record = new DatabaseRecord("dummy") { 
                Shards = new[]
                    {
                        new DatabaseTopology(), 
                        new DatabaseTopology(), 
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                    },
                BucketMigrations = new Dictionary<int, Raven.Client.ServerWide.BucketMigration>()
            };
            
            //record.ShardAllocations = PopulateRanges(record.Shards.Length);
            record.ShardAllocations = PopulateRangesEvenly(record.Shards.Length);
            var reports = CreateShardReports(record);
            var totalMovedBytes = 0L;
            Console.WriteLine($"Inital:");
            Console.WriteLine(string.Join(Environment.NewLine,reports.Select(r=>$"{r.Shard}:{new Size(r.TotalSize, SizeUnit.Bytes)}")));

            var policy = new MigrationPolicy { SizeThreshold = 10 * 1024 * 1024 };
            var moves = 0;
            var logs = new StringBuilder();
            var migrations = new HashSet<ShardMigrationResult>();
            while (moves < ShardHelper.NumberOfBuckets)
            {
                var needToMove = BucketsMigrator.NeedBalanceForDatabase(record, reports, policy, BucketsMigrator.EdgeMove, out var result);
                if (needToMove == false)
                    break;

                if (migrations.Add(result) == false)
                {
                    BucketsMigrator.NeedBalanceForDatabase(record, reports, policy, BucketsMigrator.NaiveMove, out result);
                    if(migrations.Add(result) == false)
                        throw new InvalidOperationException($"Gave up! {Environment.NewLine}{logs}");
                }

                logs.AppendLine(result.ToString());
                record.MoveBucket(result.Bucket, result.DestinationShard);
                reports[result.SourceShard].ReportPerBucket.Remove(result.Bucket, out var bucketReport);
                reports[result.DestinationShard].ReportPerBucket[result.Bucket] = bucketReport;
                totalMovedBytes += bucketReport!.Size;
                moves++;
                if (moves % 100 == 0)
                {
                    Console.WriteLine($"After {moves} moves");
                    Console.WriteLine($"ranges: {record.ShardAllocations.Count}");
                    Console.WriteLine($"So far moved: {new Size(totalMovedBytes, SizeUnit.Bytes)}");
                    Console.WriteLine(string.Join(Environment.NewLine,reports.Select(r=>$"{r.Shard}:{new Size(r.TotalSize, SizeUnit.Bytes)}")));
                }
                
                if (moves % 16 == 0)
                    migrations.Clear();

                /*for (int i = 0; i < ShardHelper.NumberOfBuckets - 1; i++)
                {
                    var shard = ShardHelper.GetShardIndex(record, i);
                    if (reports[shard].ReportPerBucket.ContainsKey(i) == false)
                        throw new InvalidOperationException();
                }*/
            }

            Console.WriteLine($"Done after {moves} moves");
        }
    }
}
