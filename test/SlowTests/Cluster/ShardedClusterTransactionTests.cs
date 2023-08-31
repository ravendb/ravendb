using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ShardedClusterTransactionTests : ReplicationTestBase
    {
        public ShardedClusterTransactionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.ClusterTransactions)]
        [InlineData(1, 1, false)]
        [InlineData(2, 1, false)]
        [InlineData(1, 2, false)]
        [InlineData(2, 2, false)]
        [InlineData(1, 1, true)]
        [InlineData(2, 1, true)]
        [InlineData(1, 2, true)]
        [InlineData(2, 2, true)]
        public async Task ShardedClusterTransaction_ChangeVector(int numberOfNodes, int numberOfShards, bool disableAtomicGuard)
        {
            var (_, leader) = await CreateRaftCluster(numberOfNodes, false, watcherCluster: true);
            var options = Options.ForMode(RavenDatabaseMode.Sharded);
            options.Server = leader;
            options.ReplicationFactor = 2;
            options.ModifyDatabaseRecord = r =>
            {
                r.Sharding ??= new ShardingConfiguration();
                r.Sharding.Shards = Enumerable.Range(0, numberOfShards)
                    .Select((shardNumber) => new KeyValuePair<int, DatabaseTopology>(shardNumber, new DatabaseTopology())).ToDictionary(x => x.Key, x => x.Value);
            };

            using var store = Sharding.GetDocumentStore(options);
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide,
                DisableAtomicDocumentWritesInClusterWideTransaction = disableAtomicGuard
            }))
            {
                var entities = new List<TestObj>();
                for (int i = 0; i < 10; i++)
                {
                    var testObj = new TestObj();
                    entities.Add(testObj);
                    await session.StoreAsync(testObj, $"TestObjs/{i}");
                }

                await session.SaveChangesAsync();

                using var session2 = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    DisableAtomicDocumentWritesInClusterWideTransaction = disableAtomicGuard
                });
                foreach (var testObj in entities)
                {
                    var changeVector = session.Advanced.GetChangeVectorFor(testObj);
                    var loadTestObj = await session2.LoadAsync<TestObj>(testObj.Id);
                    var loadChangeVector = session2.Advanced.GetChangeVectorFor(loadTestObj);
                    Assert.Equal(loadChangeVector, changeVector);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Sharding | RavenTestCategory.ClusterTransactions)]
        [InlineData(TransactionMode.ClusterWide)]
        [InlineData(TransactionMode.SingleNode)]
        public async Task ShardedClusterTransactionAndResharding(TransactionMode modifyMode)
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = TransactionMode.ClusterWide,
                       }))
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = $"test {i}"
                        }, $"Users/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                // ensure the document was created
                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<User>($"Users/1");
                }

                await Sharding.Resharding.MoveShardForId(store, "Users/1");

                using (var session = store.OpenAsyncSession(new SessionOptions
                       {
                           TransactionMode = modifyMode,
                       }))
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var u = await session.LoadAsync<User>($"Users/{i}");
                        u.Age = i;
                    }

                    await session.SaveChangesAsync();
                }
            }
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }
    }
}
