using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
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

        [RavenTheory(RavenTestCategory.Sharding)]
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

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanCreateClusterTransactionRequest()
        {
            await base.CanCreateClusterTransactionRequest();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanCreateClusterTransactionRequest2()
        {
            await base.CanCreateClusterTransactionRequest2();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ServeSeveralClusterTransactionRequests()
        {
            await base.ServeSeveralClusterTransactionRequests();
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task CanPreformSeveralClusterTransactions(int numberOfNodes)
        {
            await base.CanPreformSeveralClusterTransactions(numberOfNodes);
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ClusterTransactionWaitForIndexes(int docs)
        {
            await base.ClusterTransactionWaitForIndexes(docs);
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Export")]
        public async Task CanImportExportAndBackupWithClusterTransactions()
        {
            await base.CanImportExportAndBackupWithClusterTransactions();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task TestSessionSequance()
        {
            await base.TestSessionSequance();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:42605/databases/ResolveInFavorOfClusterTransaction_1/admin/tasks/external-replication?raft-request-id=65234c2b-a023-4954-889d-cb0975909f11, the database is sharded, but no shared route is defined for this operation!")]
        public async Task ResolveInFavorOfClusterTransaction()
        {
            await base.ResolveInFavorOfClusterTransaction();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task TestCleanUpClusterState()
        {
            await base.TestCleanUpClusterState();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task TestConcurrentClusterSessions()
        {
            await base.TestConcurrentClusterSessions();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task TestSessionMixture()
        {
            await base.TestSessionMixture();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CreateUniqueUser()
        {
            await base.CreateUniqueUser();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task SessionCompareExchangeCommands()
        {
            await base.SessionCompareExchangeCommands();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterTxWithCounters()
        {
            await base.ClusterTxWithCounters();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public void ThrowOnClusterTransactionWithCounters()
        {
            base.ThrowOnClusterTransactionWithCounters();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public void ThrowOnClusterTransactionWithAttachments()
        {
            base.ThrowOnClusterTransactionWithAttachments();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public void ThrowOnClusterTransactionWithTimeSeries()
        {
            base.ThrowOnClusterTransactionWithTimeSeries();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "nable to run request http://127.0.0.1:43631/databases/ModifyDocumentWithRevision_1/admin/revisions/config?raft-request-id=3bed2e80-bbfe-4bf5-917c-356bb9f53ccb, the database is sharded, but no shared route is defined for this operation!")]
        public async Task ModifyDocumentWithRevision()
        {
            await base.ModifyDocumentWithRevision();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task PutDocumentInDifferentCollectionWithRevision()
        {
            await base.PutDocumentInDifferentCollectionWithRevision();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:34165/databases/PutDocumentInDifferentCollection_1/admin/revisions/config?raft-request-id=82a7a3ff-f829-449f-ba02-1f8a992e1196, the database is sharded, but no shared route is defined for this operation!")]
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
        [RavenFact(RavenTestCategory.Sharding, Skip = "The database 'ClusterTransactionRequestWithRevisions_1' is sharded, can't call this method directly")]
        public async Task ClusterTransactionRequestWithRevisions()
        {
            await base.ClusterTransactionRequestWithRevisions();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ThrowOnUnsupportedOperations()
        {
            await base.ThrowOnUnsupportedOperations();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ThrowOnOptimisticConcurrency()
        {
            await base.ThrowOnOptimisticConcurrency();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ThrowOnOptimisticConcurrencyForSingleDocument()
        {
            await base.ThrowOnOptimisticConcurrencyForSingleDocument();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ThrowOnInvalidTransactionMode()
        {
            await base.ThrowOnInvalidTransactionMode();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanAddNullValueToCompareExchange()
        {
            await base.CanAddNullValueToCompareExchange();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetListCompareExchange()
        {
            await base.CanGetListCompareExchange();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:36453/databases/ClusterTransactionConflict_1/admin/tasks/external-replication?raft-request-id=6ec7f3cd-c8ee-4f1e-a01a-f38cffa10ffb, the database is sharded, but no shared route is defined for this operation!")]

        public async Task ClusterTransactionConflict()
        {
            await base.ClusterTransactionConflict();
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(@"
")]
        public async Task ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(string id)
        {
            await base.ClusterWideTransaction_WhenStoreDocWithEmptyStringId_ShouldThrowInformativeError(id);
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ClusterTransactionShouldBeRedirectedFromPromotableNode()
        {
            await base.ClusterTransactionShouldBeRedirectedFromPromotableNode();
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShardedClusterTransaction_WhenStoreTwoDocsToTwoSahrdsInTwoTrxAndTryToGetTheFirst_ShouldNotStuck()
        {
            const string firstDocId = "testObjs/0";
            
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            using var watcherStore = GetDocumentStore(new Options
            {
                Server = nodes.Single(n => n.ServerStore.NodeTag != n.ServerStore.LeaderTag), 
                ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true,
            });
            using var leaderStore = new DocumentStore
            {
                Database = watcherStore.Database, Urls = new[] {leader.WebUrl}, Conventions = new DocumentConventions {DisableTopologyUpdates = true}
            }.Initialize();
            
            for (int i = 0; i < 100; i++)
            {
                var id = $"testObjs/{i}";
                using (var session = watcherStore.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    await session.StoreAsync(new TestObj{Prop = id}, id);
                    await session.SaveChangesAsync();
                }

                await CheckCanLoad(watcherStore, id);
                await CheckCanLoad(leaderStore, id);
            }

            async Task CheckCanLoad(IDocumentStore store, string savedId)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var tokenSource = new CancellationTokenSource();
                    tokenSource.CancelAfter(TimeSpan.FromSeconds(Debugger.IsAttached ? 10 : 180));
                    _ = await session.LoadAsync<TestObj>(firstDocId, tokenSource.Token);
                    var loaded = await session.LoadAsync<TestObj>(savedId, tokenSource.Token);
                    Assert.NotNull(loaded);
                    Assert.Equal(savedId, loaded.Prop);
                }
            }
        }
    }
}
