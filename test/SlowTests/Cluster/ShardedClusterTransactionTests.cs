using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Sparrow.Utils;
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

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            options = Options.ForMode(RavenDatabaseMode.Sharded, options);
            if (options.DatabaseMode != RavenDatabaseMode.Sharded)
                throw new InvalidOperationException($"Can't run sharding test with '{options.DatabaseMode}' mode");

            return base.GetDocumentStore(options, caller);
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
                r.Shards = Enumerable.Range(0, numberOfShards).Select(_ => new DatabaseTopology()).ToArray();

            using var store = GetDocumentStore(options);
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

        [RavenFact(RavenTestCategory.Sharding, Skip = "Fix it")]
        public override Task CanImportExportAndBackupWithClusterTransactions()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after backup");
            throw new NotImplementedException();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:42605/databases/ResolveInFavorOfClusterTransaction_1/admin/tasks/external-replication?raft-request-id=65234c2b-a023-4954-889d-cb0975909f11, the database is sharded, but no shared route is defined for this operation!")]
        public override Task ResolveInFavorOfClusterTransaction()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after RavenDB-18336");
            throw new NotImplementedException();
        }
    

        [RavenFact(RavenTestCategory.Sharding, Skip = "nable to run request http://127.0.0.1:43631/databases/ModifyDocumentWithRevision_1/admin/revisions/config?raft-request-id=3bed2e80-bbfe-4bf5-917c-356bb9f53ccb, the database is sharded, but no shared route is defined for this operation!")]
        public override Task ModifyDocumentWithRevision()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after RavenDB-18336");
            throw new NotImplementedException();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:34165/databases/PutDocumentInDifferentCollection_1/admin/revisions/config?raft-request-id=82a7a3ff-f829-449f-ba02-1f8a992e1196, the database is sharded, but no shared route is defined for this operation!")]
        public override Task PutDocumentInDifferentCollection()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after RavenDB-18336");
            throw new NotImplementedException();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "Unable to run request http://127.0.0.1:36453/databases/ClusterTransactionConflict_1/admin/tasks/external-replication?raft-request-id=6ec7f3cd-c8ee-4f1e-a01a-f38cffa10ffb, the database is sharded, but no shared route is defined for this operation!")]

        public override Task ClusterTransactionConflict()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after RavenDB-18336");
            throw new NotImplementedException();
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "System.InvalidOperationExceptio The database 'ClusterTransactionRequestWithRevisions_39' is sharded, can't call this method directly")]
        public override Task ClusterTransactionRequestWithRevisions()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"Handle this after RavenDB-18336");
            throw new NotImplementedException();
        }
    }
}
