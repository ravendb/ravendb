using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Web.Studio.Processors;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class StudioBucketsTests : ClusterTestBase
    {
        public StudioBucketsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task GetBucketsView()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                var dict = new Dictionary<int, ShardBucketsStats>(); //shard to bucket stats
                var executor = store.GetRequestExecutor();
                using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    var sharding = await Sharding.GetShardingConfigurationAsync(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var name = "s";
                        for (int i = 0; i < 20; i++)
                        {
                            var id = $"user/{i}";
                            var bucket = Sharding.GetBucket(sharding, id);
                            var shardNumberForDoc = await Sharding.GetShardNumberForAsync(store, id);
                            var user = new User()
                            {
                                Name = name
                            };
                            name += "s";

                            await session.StoreAsync(user, id);

                            if (dict.TryGetValue(shardNumberForDoc, out var shardBucketStats))
                            {
                                shardBucketStats.DocCount++;
                                shardBucketStats.Buckets.Add(bucket);
                            }
                            else
                            {
                                dict.Add(shardNumberForDoc, new ShardBucketsStats() {Buckets = new HashSet<int>() {bucket}, DocCount = 1, TotalSize = 0,});
                            }
                        }

                        await session.SaveChangesAsync();
                    }

                    for (int i = 0; i < 20; i++)
                    {
                        var id = $"user/{i}";
                        var shardNumberForDoc = await Sharding.GetShardNumberForAsync(store, id);
                        var cmd = new GetDocumentSizeCommand(id);
                        executor.Execute(cmd, ctx);
                        var size = cmd.Result.ActualSize;
                        dict[shardNumberForDoc].TotalSize += size;
                    }

                    foreach (var shardNumber in sharding.Shards.Keys)
                    {
                        var shardRanges = await store.Operations.SendAsync(new GetBucketsOperation(shardNumber: shardNumber));

                        Assert.Equal(dict[shardNumber].DocCount, shardRanges.BucketRanges.Sum(x => x.Value.DocumentsCount));
                        Assert.Equal(dict[shardNumber].TotalSize, shardRanges.TotalSize);
                        Assert.Equal(dict[shardNumber].TotalSize, shardRanges.BucketRanges.Sum(x => x.Value.RangeSize));
                        Assert.Equal(dict[shardNumber].Buckets.Count, shardRanges.BucketRanges.Values.Sum(x => x.NumberOfBuckets));
                        Assert.True(shardRanges.BucketRanges.Values.All(x => x.ShardNumbers.Count == 1 && x.ShardNumbers.Contains(shardNumber)));
                    }
                }

                var results = await store.Operations.SendAsync(new GetBucketsOperation());
                Assert.Equal(20, results.BucketRanges.Values.Sum(x => x.DocumentsCount));
                
                results = await store.Operations.SendAsync(new GetBucketsOperation(range: 1));
                foreach (var stats in dict.Values)
                {
                    var shardDocsSize = 0L;
                    foreach (var bucket in stats.Buckets)
                    {
                        Assert.True(results.BucketRanges.ContainsKey(bucket));
                        Assert.Equal(bucket, results.BucketRanges[bucket].FromBucket);
                        Assert.Equal(bucket, results.BucketRanges[bucket].ToBucket);
                        shardDocsSize += results.BucketRanges[bucket].RangeSize;
                    }
                    Assert.Equal(stats.TotalSize, shardDocsSize);
                }

                Assert.Equal(dict.Sum(x => x.Value.TotalSize), results.TotalSize);

                results = await store.Operations.SendAsync(new GetBucketsOperation(range: int.MaxValue));
                Assert.Equal(1, results.BucketRanges.Count);
                Assert.Equal(3, results.BucketRanges[0].ShardNumbers.Count);
                Assert.Equal(dict.Sum(x => x.Value.TotalSize), results.TotalSize);
                Assert.Equal(20, results.BucketRanges.Sum(x => x.Value.DocumentsCount));
            }
        }

        public class ShardBucketsStats
        {
            public int TotalSize;
            public int DocCount;
            public HashSet<int> Buckets;
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task CanGetBucketInfo()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                var bucketInfos = new Dictionary<int, BucketInfo>();
                var sharding = await Sharding.GetShardingConfigurationAsync(store);

                var executor = store.GetRequestExecutor();
                using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 20; i++)
                        {
                            var id = $"user/{i}";
                            var bucket = Sharding.GetBucket(sharding, id);

                            await session.StoreAsync(new User(), id);

                            if (bucketInfos.TryGetValue(bucket, out var bucketInfo))
                            {
                                bucketInfo.Items.Add(id);
                                bucketInfo.NumberOfDocuments += 1;
                            }
                            else
                            {
                                bucketInfos.Add(bucket, new BucketInfo()
                                {
                                    Bucket = bucket,
                                    Items = new List<string>() { "Document "  + id },
                                    NumberOfDocuments = 1
                                });
                            }
                        }

                        await session.SaveChangesAsync();

                        for (int i = 0; i < 20; i++)
                        {
                            var id = $"user/{i}";
                            var bucket = Sharding.GetBucket(sharding, id);
                            var cmd = new GetDocumentSizeCommand(id);
                            executor.Execute(cmd, ctx);
                            var docSize = cmd.Result.ActualSize;

                            bucketInfos[bucket].Size += docSize;
                        }
                    }
                }
                
                foreach (var (bucket, info) in bucketInfos)
                {
                    var results = await store.Operations.SendAsync(new GetBucketInfoOperation(bucket));
                    Assert.Equal(info.Bucket, results.Bucket);
                    Assert.Equal(info.Items.Count, results.NumberOfDocuments);
                    Assert.True(EnumerableExtension.ElementsEqual(info.Items, results.Items));
                    Assert.Equal(info.Size, results.Size);
                }
            }
        }
    }

    public class GetBucketsOperation : IOperation<BucketsResults>
    {
        private readonly int _fromBucket;
        private readonly int _toBucket;
        private readonly int _range;
        private readonly int? _shardNumber;

        public GetBucketsOperation(int fromBucket = 0, int toBucket = int.MaxValue, int range = 32 * 1024, int? shardNumber = null)
        {
            _fromBucket = fromBucket;
            _toBucket = toBucket;
            _range = range;
            _shardNumber = shardNumber;
        }

        public RavenCommand<BucketsResults> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetBucketsCommand(_fromBucket, _toBucket, _range, _shardNumber);
        }
    }

    public class GetBucketInfoOperation : IOperation<BucketInfo>
    {
        private readonly int _bucket;
        
        public GetBucketInfoOperation(int bucket)
        {
            _bucket = bucket;
        }
        
        public RavenCommand<BucketInfo> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetBucketInfoCommand(_bucket);
        }
    }
}
