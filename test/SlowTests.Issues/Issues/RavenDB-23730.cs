using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_23730 : ReplicationTestBase
    {
        public RavenDB_23730(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Revisions)]
        public async Task ReshardingBeforeBackupTest()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using var src = Sharding.GetDocumentStore();

            await RevisionsHelper.SetupRevisionsAsync(src, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

            const string id = "Users/1";
            using (var session = src.OpenSession())
            {
                session.Store(new User { Name = "Old" }, id);
                session.SaveChanges();
            }

            using (var session = src.OpenSession())
            {
                var user = session.Load<User>(id);
                Assert.NotNull(user);
                user.Name = "New";
                session.SaveChanges();
            }

            var oldLocation = await Sharding.GetShardNumberForAsync(src, id);
            await Sharding.Resharding.MoveShardForId(src, id, toShard: Math.Abs(oldLocation - 1));

            var nodes = new List<RavenServer>() { Server };
            var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(nodes, src.Database);
            var config = Backup.CreateBackupConfiguration(backupPath);
            await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(nodes, src, config);
            Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

            var dirs = Directory.GetDirectories(backupPath);
            var sharding = await Sharding.GetShardingConfigurationAsync(src);
            var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

            var restoredDatabaseName = $"restored_{Guid.NewGuid()}-{src.Database}";
            using (Sharding.Backup.ReadOnly(backupPath))
            using (Backup.RestoreDatabase(src, new RestoreBackupConfiguration { DatabaseName = restoredDatabaseName, ShardRestoreSettings = settings },
                       timeout: TimeSpan.FromSeconds(60)))
            {
                // Wait until 'shardDb' success to perform 'ExecuteMoveDocumentsAsync'
                var bucket = await Sharding.GetBucketAsync(src, id, restoredDatabaseName);
                await Sharding.Resharding.WaitForMigrationComplete(src, bucket, restoredDatabaseName);

                using (var session = src.OpenAsyncSession(restoredDatabaseName))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("New", user.Name);
                    var revCount = await session.Advanced.Revisions.GetCountForAsync(id);
                    Assert.Equal(2, revCount);

                    var user1Revisions = await GetRevisionsCvs(session, id);
                    Assert.Equal(2, user1Revisions.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        public async Task IncomingReplicationRecreateRevisionAltoughtHavingTombstoneForIt()
        {
            var options = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString();
                }
            };
            using var store1 = GetDocumentStore(options);
            using var store2 = GetDocumentStore(options);
            using var store3 = GetDocumentStore(options);

            var revisionsConfig = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>() { ["Users"] = new RevisionsCollectionConfiguration() { Disabled = false } }
            };
            await RevisionsHelper.SetupRevisionsAsync(store1, store1.Database, revisionsConfig);
            await RevisionsHelper.SetupRevisionsAsync(store2, store2.Database, revisionsConfig);
            await RevisionsHelper.SetupRevisionsAsync(store3, store3.Database, revisionsConfig);

            List<string> user1Revisions;
            var user1 = new User { Id = "Users/1-A", Name = "Shahar_old" };
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();

                user1Revisions = await GetRevisionsCvs(session, user1.Id);
                Assert.Equal(1, user1Revisions.Count);
            }

            var ongoingTaskId12 = (await SetupReplicationAsync(store1, store2)).First().TaskId;
            await EnsureReplicatingAsync(store1, store2);
            // 'Shahar_old' revision is now on store1 and store2, store3 still has nothing

            using (var session = store1.OpenAsyncSession())
            {
                (await session.LoadAsync<User>(user1.Id)).Name = "Shahar_new";
                await session.SaveChangesAsync();
            }

            await EnsureReplicatingAsync(store1, store2);
            // 'Shahar_old' and 'Shahar_new' revisions are now on store1 and store2, store3 still has nothing

            await ModifyExternalReplication(from: store1, to: store2, ongoingTaskId12, disable: true);

            await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(user1.Id, user1Revisions));
            // store1 now has revision 'Shahar_new' and 'Shahar_old' revision-tombstone, store2 has revisions 'Shahar_old' and 'Shahar_new', store3 still has nothing
            var stats = await GetDatabaseStatisticsAsync(store1, store1.Database);
            Assert.Equal(1, stats.CountOfTombstones);
            Assert.Equal(1, stats.CountOfRevisionDocuments);

            // 1=>3
            await SetupReplicationAsync(store1, store3);
            await EnsureReplicatingAsync(store1, store3);
            // store3 now has revision 'Shahar_new' and 'Shahar_old' revision-tombstone (got it from store1), store1 now has revision 'Shahar_new' and 'Shahar_old' revision-tombstone, store2 has revisions 'Shahar_old' and 'Shahar_new'.
            stats = await GetDatabaseStatisticsAsync(store3, store3.Database);
            Assert.Equal(1, stats.CountOfTombstones);
            Assert.Equal(1, stats.CountOfRevisionDocuments);

            // 2=>3
            await SetupReplicationAsync(store2, store3);
            await EnsureReplicatingAsync(store2, store3);
            // [Before the fix]
            // store3 now has revision 'Shahar_new' , 'Shahar_old' revision-tombstone and 'Shahar_old' revision (got it from store2)
            // store1 now has revision 'Shahar_new' and 'Shahar_old' revision-tombstone,
            // store2 has revisions 'Shahar_old' and 'Shahar_new'.

            // 2=>1
            await SetupReplicationAsync(store2, store1);
            await EnsureReplicatingAsync(store2, store1);
            // [Before the fix]
            // store3 now has revision 'Shahar_new' , 'Shahar_old' revision-tombstone and 'Shahar_old' revision (got it from store2)
            // store2 has revisions 'Shahar_old' and 'Shahar_new'.
            // store1 now has revision 'Shahar_new' , 'Shahar_old' revision-tombstone and 'Shahar_old' revision

            // 1=>2 (enable)
            await ModifyExternalReplication(from: store1, to: store2, ongoingTaskId12, disable: false);
            // 3=>1
            await SetupReplicationAsync(store3, store1);
            await EnsureReplicatingAsync(store3, store1);
            // 3=>2
            await SetupReplicationAsync(store3, store2);
            await EnsureReplicatingAsync(store3, store2);

            // Replication: 1=>3, 1=>2, 2=>3, 2=>1, 3=>1, 3=>2

            stats = await GetDatabaseStatisticsAsync(store1, store1.Database);
            Assert.Equal(1, stats.CountOfTombstones);
            Assert.Equal(1, stats.CountOfRevisionDocuments);

            stats = await GetDatabaseStatisticsAsync(store2, store2.Database);
            Assert.Equal(1, stats.CountOfTombstones);
            Assert.Equal(1, stats.CountOfRevisionDocuments);

            stats = await GetDatabaseStatisticsAsync(store3, store3.Database);
            Assert.Equal(1, stats.CountOfTombstones);
            Assert.Equal(1, stats.CountOfRevisionDocuments);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task CanRecreateForceCreatedRevision()
        {
            var options = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString();
                }
            };
            using var store1 = GetDocumentStore(options);

            List<string> revisions;

            var user1 = new User { Id = "Users/1-A", Name = "Shahar_old" };
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();

                session.Advanced.Revisions.ForceRevisionCreationFor(id: user1.Id);
                await session.SaveChangesAsync();

                revisions = await GetRevisionsCvs(session, user1.Id);
                Assert.Equal(1, revisions.Count);
            }

            await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(user1.Id, revisions, removeForceCreatedRevisions: true));

            using (var session = store1.OpenAsyncSession())
            {
                var revisions2 = await GetRevisionsCvs(session, user1.Id);
                Assert.Empty(revisions2);

                session.Advanced.Revisions.ForceRevisionCreationFor(id: user1.Id);
                await session.SaveChangesAsync();

                var doc = await session.LoadAsync<User>(user1.Id);
                var cv = session.Advanced.GetChangeVectorFor(doc);

                var revisions3 = await GetRevisionsCvs(session, user1.Id);
                Assert.Equal(1, revisions3.Count);
                Assert.Equal(cv, revisions3.First());
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Revisions)]
        public async Task CanRecreateForceCreatedRevisionAndReplicate()
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using var store = GetDocumentStore(new Options()
            {
                Server = leader,
                ReplicationFactor = 3,
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = 1.ToString();
                }
            });

            List<string> revisions;

            var user1 = new User { Id = "Users/1-A", Name = "Shahar_old" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.SaveChangesAsync();

                session.Advanced.Revisions.ForceRevisionCreationFor(id: user1.Id);
                await session.SaveChangesAsync();

                revisions = await GetRevisionsCvs(session, user1.Id);
                Assert.Equal(1, revisions.Count);
            }

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(user1.Id, revisions, removeForceCreatedRevisions: true));

            using (var session = store.OpenAsyncSession())
            {
                var revisions2 = await GetRevisionsCvs(session, user1.Id);
                Assert.Empty(revisions2);

                session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(15));

                session.Advanced.Revisions.ForceRevisionCreationFor(id: user1.Id);
                await session.SaveChangesAsync();
            }

            foreach (var n in nodes)
            {
                using var nodeStore = GetStoreForServer(n, store.Database);
                // Wait for (force-created) revision replication
                await WaitForValueAsync(async () =>
                {
                    using (var session = nodeStore.OpenAsyncSession())
                    {
                        return (await GetRevisionsCvs(session, user1.Id)).Count;
                    }
                }, 1);
            }

            IDocumentStore GetStoreForServer(RavenServer server, string database)
            {
                return new DocumentStore
                    {
                        Database = database, 
                        Urls = new[] { server.WebUrl }, 
                        Conventions = new DocumentConventions { DisableTopologyUpdates = true }
                    }
                    .Initialize();
            }
        }

        private async Task ModifyExternalReplication(DocumentStore from, DocumentStore to, long ongoingTaskId, bool disable)
        {
            var external = new ExternalReplication(from.Database, $"ConnectionString-{to.Identifier}") { TaskId = ongoingTaskId, Disabled = disable };
            await from.Maintenance.SendAsync(new UpdateExternalReplicationOperation(external));
        }

        private async Task<List<string>> GetRevisionsCvs(IAsyncDocumentSession session, string id)
        {
            var cvs = (await session
                .Advanced
                .Revisions
                .GetMetadataForAsync(id)).Select(m => m.GetString(Constants.Documents.Metadata.ChangeVector));

            return cvs.ToList();
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
