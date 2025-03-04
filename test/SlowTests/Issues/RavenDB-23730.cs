using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23730 : ReplicationTestBase
    {
        public RavenDB_23730(ITestOutputHelper output) : base(output)
        {
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

                var revisions3 = await GetRevisionsCvs(session, user1.Id);
                Assert.Equal(1, revisions3.Count);
                Assert.Equal(revisions.First(), revisions3.First());
            }
        }

        private async Task ModifyExternalReplication(DocumentStore from, DocumentStore to, long ongoingTaskId, bool disable)
        {
            var external = new ExternalReplication(from.Database, $"ConnectionString-{to.Identifier}") { TaskId = ongoingTaskId, Disabled = true };
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
