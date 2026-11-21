using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Revisions
{
    // Revision keys on a database created by this build. The cases that start from 6.2 data are in
    // InterversionTests.Revisions.RavenDB_27675.
    public class RavenDB_27675 : ReplicationTestBase
    {
        public RavenDB_27675(ITestOutputHelper output) : base(output)
        {
        }

        // A database created by this build is hashed from the start: HashedRevisionPk in its storage and no cleanup started.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DatabaseCreatedOnCurrent_NoCleanupToRun()
        {
            using (var store = GetDocumentStore())
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, "users/regular", from: 0, count: 3);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tx = context.Transaction.InnerTransaction;
                    Assert.True(RevisionsStorage.AllRevisionRowsHashed(tx), "the storage of a database created by this build has HashedRevisionPk");
                    Assert.False(CleanupState.TryRead(tx, out _, out _), "the revisions key cleanup never ran, so it stored no state");
                }
            }
        }

        // A document deleted and recreated in another collection has revisions in two collection tables; deleting its revisions
        // must delete both.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DocumentRecreatedInAnotherCollection_DeleteRevisions_DeletesEveryRevision()
        {
            const string id = "users/moved";
            using (var store = GetDocumentStore())
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await RecreateInAnotherCollectionAsync(store, id);
                var before = await RevisionCvsAsync(store, id);
                Assert.True(before.Count == 4, $"precondition: 2 user revisions, the delete revision and the employee revision, got {before.Count}");

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(id));

                var left = await RevisionCvsAsync(store, id);
                Assert.True(left.Count == 0, $"{left.Count} revisions left after deleting all: '{string.Join("', '", left)}'");
                await AssertListedRevisionsMatchCountAsync(store, id);
            }
        }

        // A replicated revision tombstone must delete a revision stored in another collection's table (the document was deleted
        // and recreated in another collection), and replication must go on.
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task DocumentRecreatedInAnotherCollection_HubDeletesRevisions_SinkDeletesThemAndKeepsReplicating()
        {
            const string id = "users/moved";
            using (var hubStore = GetDocumentStore())
            using (var sinkStore = GetDocumentStore())
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupReplicationAsync(hubStore, sinkStore);

                await RecreateInAnotherCollectionAsync(hubStore, id);
                await EnsureReplicatingAsync(hubStore, sinkStore);
                Assert.True((await RevisionCvsAsync(sinkStore, id)).Count == 4, "precondition: the sink has the 4 revisions");

                await hubStore.Maintenance.SendAsync(new DeleteRevisionsOperation(id));
                await EnsureReplicatingAsync(hubStore, sinkStore);

                var left = await RevisionCvsAsync(sinkStore, id);
                Assert.True(left.Count == 0, $"the hub deleted the revisions, the sink still has {left.Count}: '{string.Join("', '", left)}'");
                await AssertListedRevisionsMatchCountAsync(sinkStore, id);
            }
        }

        // As DocumentRecreatedInAnotherCollection_HubDeletesRevisions_SinkDeletesThemAndKeepsReplicating, by import: B imports A's
        // documents and revisions, A deletes the revisions, and A's revision tombstones imported into B must delete them all,
        // without import errors.
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Smuggler)]
        public async Task DocumentRecreatedInAnotherCollection_ImportRevisionTombstones_DeletesTheRevisions()
        {
            const string id = "users/moved";
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await ConfigureUsersRevisionsAsync(storeA, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(storeB, minToKeep: 100);
                await RecreateInAnotherCollectionAsync(storeA, id);
                await ExportImportAsync(storeA, storeB, DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments);
                var onA = await RevisionCvsAsync(storeA, id);
                Assert.True(onA.All((await RevisionCvsAsync(storeB, id)).Contains), $"precondition: B has A's {onA.Count} revisions of '{id}'");

                await storeA.Maintenance.SendAsync(new DeleteRevisionsOperation(id));
                var result = await ExportImportAsync(storeA, storeB, DatabaseItemType.Tombstones);
                Assert.Equal(0, result.Tombstones.ErroredCount);

                var left = (await RevisionCvsAsync(storeB, id)).Where(onA.Contains).ToList();
                Assert.True(left.Count == 0, $"A deleted the revisions of '{id}', B still has '{string.Join("', '", left)}'");
            }
        }

        private static Task ConfigureUsersRevisionsAsync(IDocumentStore store, int minToKeep)
        {
            return store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 100 },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = minToKeep }
                }
            }));
        }

        private static async Task WriteVersionsAsync(IDocumentStore store, string id, int from, int count)
        {
            for (var i = from; i < from + count; i++)
            {
                using var session = store.OpenAsyncSession();
                var user = await session.LoadAsync<User>(id);
                if (user == null)
                    await session.StoreAsync(user = new User(), id);
                user.Name = "v" + i;
                await session.SaveChangesAsync();
            }
        }

        // Two user revisions, the delete revision, then an employee revision under the same id: revisions in two collection tables.
        private static async Task RecreateInAnotherCollectionAsync(IDocumentStore store, string id)
        {
            await WriteVersionsAsync(store, id, from: 0, count: 2);
            using (var s = store.OpenAsyncSession())
            {
                s.Delete(id);
                await s.SaveChangesAsync();
            }
            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Employee { FirstName = "Moved" }, id);
                await s.SaveChangesAsync();
            }
        }

        private static async Task<List<string>> RevisionCvsAsync(IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 100);
            return metadata.Select(m => m.GetString("@change-vector")).ToList();
        }

        private static async Task AssertListedRevisionsMatchCountAsync(IDocumentStore store, string id)
        {
            using var session = store.OpenAsyncSession();
            var listed = (await session.Advanced.Revisions.GetMetadataForAsync(id, pageSize: 1000)).Count;
            var count = await session.Advanced.Revisions.GetCountForAsync(id);
            Assert.True(listed == count, $"'{id}': {listed} revisions listed but the revision count is {count}");
        }

        private async Task<SmugglerResult> ExportImportAsync(IDocumentStore source, IDocumentStore destination, DatabaseItemType types)
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");
            var export = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = types }, file);
            await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            var import = await destination.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = types }, file);
            return await import.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromMinutes(1));
        }
    }
}
