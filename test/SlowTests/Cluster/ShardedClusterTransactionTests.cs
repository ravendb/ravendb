using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ShardedClusterTransactionTests : ClusterTransactionTests
    {
        public ShardedClusterTransactionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
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

            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ModifyDatabaseRecord = r => 
                    r.Shards = Enumerable.Range(0, numberOfShards).Select(_ => new DatabaseTopology()).ToArray(),
                ReplicationFactor = 2
            });

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

                // WaitForUserToContinueTheTest(store);

                //TODO To remove waiting
                await Task.Delay(2000);
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

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            options ??= new Options();
            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            options.ModifyDatabaseRecord = r =>
            {
                modifyDatabaseRecord?.Invoke(r);
                var databaseTopology = r.Topology ?? new DatabaseTopology();
                r.Topology = null;

                r.Shards = Enumerable.Range(0, 3)
                        .Select(_ => databaseTopology)
                        .ToArray();
            };
            return base.GetDocumentStore(options, caller);
        }
    }
}
