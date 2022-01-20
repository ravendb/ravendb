using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ShardedClusterTransactionTests : ClusterTransactionTestsBase
    {
        public ShardedClusterTransactionTests(ITestOutputHelper output) : base(output)
        {
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

        [Fact]
        public async Task CanCreateClusterTransactionRequest()
        {
            await base.CanCreateClusterTransactionRequest();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task CanCreateClusterTransactionRequest2()
        {
            await base.CanCreateClusterTransactionRequest2();
        }

        [Fact]
        public async Task ServeSeveralClusterTransactionRequests()
        {
            await base.ServeSeveralClusterTransactionRequests();
        }

        [Theory(Skip = "Should complete shard implementation")]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task CanPreformSeveralClusterTransactions(int numberOfNodes)
        {
            await base.CanPreformSeveralClusterTransactions(numberOfNodes);
        }

        [Theory(Skip = "Should complete shard implementation")]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ClusterTransactionWaitForIndexes(int docs)
        {
            await base.ClusterTransactionWaitForIndexes(docs);
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task CanImportExportAndBackupWithClusterTransactions()
        {
            await base.CanImportExportAndBackupWithClusterTransactions();
        }

        [Fact]
        public async Task TestSessionSequance()
        {
            await base.TestSessionSequance();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ResolveInFavorOfClusterTransaction()
        {
            await base.ResolveInFavorOfClusterTransaction();
        }

        [Fact]
        public async Task TestCleanUpClusterState()
        {
            await base.TestCleanUpClusterState();
        }

        [Fact]
        public async Task TestConcurrentClusterSessions()
        {
            await base.TestConcurrentClusterSessions();
        }

        [Fact]
        public async Task TestSessionMixture()
        {
            await base.TestSessionMixture();
        }

        [Fact]
        public async Task CreateUniqueUser()
        {
            await base.CreateUniqueUser();
        }

        [Fact]
        public async Task SessionCompareExchangeCommands()
        {
            await base.SessionCompareExchangeCommands();
        }

        [Fact]
        public async Task ClusterTxWithCounters()
        {
            await base.ClusterTxWithCounters();
        }

        [Fact]
        public void ThrowOnClusterTransactionWithCounters()
        {
            base.ThrowOnClusterTransactionWithCounters();
        }

        [Fact]
        public void ThrowOnClusterTransactionWithAttachments()
        {
            base.ThrowOnClusterTransactionWithAttachments();
        }

        [Fact]
        public void ThrowOnClusterTransactionWithTimeSeries()
        {
            base.ThrowOnClusterTransactionWithTimeSeries();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task ModifyDocumentWithRevision()
        {
            await base.ModifyDocumentWithRevision();
        }

        [Fact]
        public async Task PutDocumentInDifferentCollectionWithRevision()
        {
            await base.PutDocumentInDifferentCollectionWithRevision();
        }

        [Fact(Skip = "Should complete shard implementation")]
        public async Task PutDocumentInDifferentCollection()
        {
            await base.PutDocumentInDifferentCollection();
        }

        /// <summary>
        /// This is a comprehensive test. The general flow of the test is as following:
        /// - Create cluster with 5 nodes with a database on _all_ of them and enable revisions.
        /// - Bring one node down, he will later be used to verify the correct behavior (our SUT).
        /// - Perform a cluster transaction which involves a document.
        /// - Bring all nodes down except of the original leader.
        /// - Bring the SUT node back up and wait for the document to replicate.
        /// - Bring another node up in order to have a majority.
        /// - Wait for the raft index on the SUT to catch-up and verify that we still have one document with one revision.
        /// </summary>
        /// <returns></returns>
        [Fact(Skip = "Should complete shard implementation")]
        public async Task ClusterTransactionRequestWithRevisions()
        {
            await base.ClusterTransactionRequestWithRevisions();
        }

        [Fact]
        public async Task ThrowOnUnsupportedOperations()
        {
            await base.ThrowOnUnsupportedOperations();
        }

        [Fact]
        public async Task ThrowOnOptimisticConcurrency()
        {
            await base.ThrowOnOptimisticConcurrency();
        }

        [Fact]
        public async Task ThrowOnOptimisticConcurrencyForSingleDocument()
        {
            await base.ThrowOnOptimisticConcurrencyForSingleDocument();
        }

        [Fact]
        public async Task ThrowOnInvalidTransactionMode()
        {
            await base.ThrowOnInvalidTransactionMode();
        }

        [Fact]
        public async Task CanAddNullValueToCompareExchange()
        {
            await base.CanAddNullValueToCompareExchange();
        }

        [Fact]
        public async Task CanGetListCompareExchange()
        {
            await base.CanGetListCompareExchange();
        }

        [Fact(Skip = "Should complete shard implementation")]

        public async Task ClusterTransactionConflict()
        {
            await base.ClusterTransactionConflict();
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(@"
")]
        public async Task ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(string id)
        {
            await base.ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(id);
        }

        [Fact]
        public async Task ClusterTransactionShouldBeRedirectedFromPromotableNode()
        {
            await base.ClusterTransactionShouldBeRedirectedFromPromotableNode();
        }
    }
}
