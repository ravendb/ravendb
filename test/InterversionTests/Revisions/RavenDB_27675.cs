using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Commands.Cluster;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace InterversionTests.Revisions
{
    // RavenDB-27675: real-flow reproduction of twin revision rows (a legacy raw-PK row and a hashed row for the same
    // revision) on databases upgraded in place from 6.2, using only client operations against real servers.
    public partial class RavenDB_27675 : MixedClusterTestBase
    {
        public RavenDB_27675(ITestOutputHelper output) : base(output)
        {
        }

        // ---- one server, created on 6.2 and upgraded in place ----

        // A database created on 7.2.6 holds only hashed revision rows, and its record has HashedRevisionPk: this build runs no revisions
        // key cleanup, and its revision tombstones are purged on the first tombstone cleaner run.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task CreatedOn726_HashOnlyFromTheFirstLoad_NoCleanup()
        {
            var ids = new[] { "users/1", "users/2" };
            var db = GetDatabaseName();
            var node = await GetServerAsync(Released72Version, customSettings: PersistentSettings);
            var before = new Dictionary<string, List<string>>();
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                foreach (var id in ids)
                {
                    await WriteVersionsAsync(store, id, clusterWide: false, from: 0, count: 3);
                    before[id] = await RevisionCvsAsync(store, id);
                }
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(ids[1]));
            }

            await UpgradeServerAsync("current", node);
            using (var store = CreateStore(node.Url, db))
            {
                var database = await DatabaseAtAsync(node.Url, db);
                ReadKeyCleanerState(database, out _, out var stored);
                Assert.False(stored, "no revisions key cleanup ran");
                Assert.True(database.DocumentsStorage.RevisionsStorage.HashedRevisionPk, "the database is hash-only from its first load");

                Assert.Equal(before[ids[0]], await RevisionCvsAsync(store, ids[0]));
                Assert.Empty(await RevisionCvsAsync(store, ids[1]));

                var tombstones = CountRevisionTombstones(database);
                Assert.True(tombstones > 0, $"precondition: the revision deletes on 7.2.6 left revision tombstones, found {tombstones}");
                await database.TombstoneCleaner.ExecuteCleanup();
                tombstones = CountRevisionTombstones(database);
                Assert.True(tombstones == 0, $"the tombstone cleaner purged the revision tombstones, {tombstones} left");
            }
        }

        // The cleanup decides from revision tombstones, so the tombstone cleaner must purge none until it is done, and all of them
        // after. Flow: on 6.2 a document's revisions are deleted; upgraded in place, another document's are deleted. Control: a
        // database created by this build purges them on the same run.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62_TombstoneCleanerKeepsRevisionTombstonesUntilTheCleanupIsDone()
        {
            var db = GetDatabaseName();
            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, "users/on-62", clusterWide: false, from: 0, count: 3);
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation("users/on-62"));
            }

            await UpgradeServerAsync("current", node);
            using (var store = CreateStore(node.Url, db))
            {
                await WriteVersionsAsync(store, "users/on-current", clusterWide: false, from: 0, count: 3);
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation("users/on-current"));
                await WaitForRevisionsKeyCleanupAsync(node.Url, db);

                var database = await DatabaseAtAsync(node.Url, db);
                var under62Keys = CountLegacyRevisionTombstones(node.Url, db, new[] { "users/on-62" });
                Assert.True(under62Keys == 0, $"walk 2 moved the 6.2 revision tombstones to their hashed keys, {under62Keys} left");
                var before = CountRevisionTombstones(database);
                Assert.True(before >= 6, $"precondition: the revision deletes left revision tombstones, found {before}");
                await database.TombstoneCleaner.ExecuteCleanup();
                var after = CountRevisionTombstones(database);
                Assert.True(after == 0, $"once the storage has HashedRevisionPk the tombstone cleaner purges the revision tombstones, before {before}, after {after}");
            }

            using (var store = GetDocumentStore())
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, "users/on-current", clusterWide: false, from: 0, count: 3);
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation("users/on-current"));

                // the upgraded server is node A too, so look the database up on the store's own server
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                Assert.True(database.DocumentsStorage.RevisionsStorage.HashedRevisionPk, "control: a database created by this build is hash-only from its first load");
                var before = CountRevisionTombstones(database);
                await database.TombstoneCleaner.ExecuteCleanup();
                var after = CountRevisionTombstones(database);
                Assert.True(before > 0 && after == 0, $"control: a database created by this build purges its revision tombstones, before {before}, after {after}");
            }
        }

        // Deleting the revisions of a document that also has an attachment tombstone must delete them all. Flow: cluster-wide
        // revisions on 6.2; upgraded in place; an attachment added and deleted.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62_DocumentHasAttachmentTombstone_DeleteRevisions_DeletesThem()
        {
            const string id = "users/cluster-tx";
            var db = GetDatabaseName();
            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, id, clusterWide: true, from: 0, count: 3);
            }

            await UpgradeServerAsync("current", node);
            using (var store = CreateStore(node.Url, db))
            {
                await AddAttachmentAsync(store, id);
                using (var s = store.OpenAsyncSession())
                {
                    s.Advanced.Attachments.Delete(id, "a.txt");
                    await s.SaveChangesAsync();
                }

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(id));

                var left = await RevisionCvsAsync(store, id);
                Assert.True(left.Count == 0, $"{left.Count} revisions left after deleting all: '{string.Join("', '", left)}'");
                await AssertListedRevisionsMatchCountAsync(store, new[] { id });
            }
        }

        // An upgraded database runs the revisions key cleanup to its end and stops (LastRawEtag is the last etag written on 6.2, and
        // each walk clears its state), and compacting it then keeps every revision, revision attachment and tombstone, and keeps the
        // storage hashed. Flow: on 6.2, regular and cluster-wide revisions, revisions with an attachment, and a document whose
        // revisions (with the attachment) are deleted; upgraded in place; the cleanup runs to its end; the database is compacted.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments | RavenTestCategory.Voron | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62_CompactedAfterTheCleanup_RevisionsAttachmentsAndTombstonesStay()
        {
            const string withAttachment = "users/with-attachment";
            const string deleted = "users/deleted";
            var ids = new[] { "users/regular", "users/cluster-tx", withAttachment };
            var db = GetDatabaseName();
            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            long lastEtagOn62;
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, ids[0], clusterWide: false, from: 0, count: 3);
                await WriteVersionsAsync(store, ids[1], clusterWide: true, from: 0, count: 3);
                foreach (var id in new[] { withAttachment, deleted })
                {
                    await WriteVersionsAsync(store, id, clusterWide: false, from: 0, count: 1);
                    await AddAttachmentAsync(store, id);
                    await WriteVersionsAsync(store, id, clusterWide: false, from: 1, count: 2);
                }
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(deleted));
                lastEtagOn62 = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).LastDatabaseEtag ?? 0;
                Assert.True(lastEtagOn62 > 0, "precondition: the 6.2 statistics give the database's last etag");
            }

            await UpgradeServerAsync("current", node);
            using (var store = CreateStore(node.Url, db))
            {
                await WaitForRevisionsKeyCleanupAsync(node.Url, db);
                var database = await DatabaseAtAsync(node.Url, db);
                Assert.Equal(lastEtagOn62, ReadKeyCleanerState(database, out _, out _).LastRawEtag);
                Assert.False(KeyCleanerWalkStateLeft(database), "each walk clears its part of the state when it ends");

                var listed = new Dictionary<string, List<string>>();
                foreach (var id in ids)
                    listed[id] = await RevisionCvsAsync(store, id);
                var withAttachmentCvs = new List<string>();
                foreach (var cv in listed[withAttachment])
                {
                    if (await HasRevisionAttachmentAsync(store, withAttachment, cv))
                        withAttachmentCvs.Add(cv);
                }

                var revisionTombstones = CountRevisionTombstones(database);
                var attachmentTombstones = CountAttachmentTombstones(database);
                var before = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.True(withAttachmentCvs.Count >= 2 && revisionTombstones > 0 && attachmentTombstones > 0,
                    $"precondition: revisions with the attachment {withAttachmentCvs.Count}, revision tombstones {revisionTombstones}, attachment tombstones {attachmentTombstones}");

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings { DatabaseName = db, Documents = true }));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                database = await DatabaseAtAsync(node.Url, db);
                ReadKeyCleanerState(database, out var allRowsHashed, out _);
                var after = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                var failures = new List<string>();
                if (allRowsHashed == false || database.DocumentsStorage.RevisionsStorage.HashedRevisionPk == false)
                    failures.Add($"the compacted storage lost HashedRevisionPk: stored {allRowsHashed}, loaded {database.DocumentsStorage.RevisionsStorage.HashedRevisionPk}");
                foreach (var id in ids)
                {
                    var now = await RevisionCvsAsync(store, id);
                    if (now.Count != listed[id].Count || now.Except(listed[id]).Any())
                        failures.Add($"'{id}': listed before '{string.Join("', '", listed[id])}', after '{string.Join("', '", now)}'");
                    foreach (var cv in listed[id])
                    {
                        using var s = store.OpenAsyncSession();
                        if (await s.Advanced.Revisions.GetAsync<User>(cv) == null)
                            failures.Add($"'{id}': revision '{cv}' not found by its change vector");
                    }
                }
                foreach (var cv in withAttachmentCvs)
                {
                    if (await HasRevisionAttachmentAsync(store, withAttachment, cv) == false)
                        failures.Add($"revision '{cv}' no longer exposes its attachment");
                }
                if (CountRevisionTombstones(database) != revisionTombstones)
                    failures.Add($"revision tombstones before {revisionTombstones}, after {CountRevisionTombstones(database)}");
                if (CountAttachmentTombstones(database) != attachmentTombstones)
                    failures.Add($"attachment tombstones before {attachmentTombstones}, after {CountAttachmentTombstones(database)}");
                if (after.CountOfRevisionDocuments != before.CountOfRevisionDocuments || after.CountOfAttachments != before.CountOfAttachments)
                    failures.Add($"revisions {before.CountOfRevisionDocuments} -> {after.CountOfRevisionDocuments}, attachments {before.CountOfAttachments} -> {after.CountOfAttachments}");

                Assert.True(failures.Count == 0, string.Join("; ", failures));
                await AssertListedRevisionsMatchCountAsync(store, ids);
            }
        }

        // A document's revision attachments that take many batches to move: each batch continues after the last row the one before
        // moved (kept in the stored key), and every revision keeps its attachments. Flow: on 6.2 a document gets 10 attachments and
        // 30 more versions and keeps its newest 10 (100 revision attachment rows, 200 attachment tombstones); upgraded in place with
        // a batch time limit of 1 ms.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62_AttachmentMoveSpansBatches_EveryRevisionKeepsItsAttachment()
        {
            const string id = "users/with-attachment";
            var db = GetDatabaseName();
            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            var withAttachment = new List<string>();
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, id, clusterWide: false, from: 0, count: 1);
                using (var s = store.OpenAsyncSession())
                {
                    s.Advanced.Attachments.Store(id, "a.txt", new MemoryStream(new byte[] { 1, 2, 3 }), "text/plain");
                    for (var i = 1; i < 10; i++)
                        s.Advanced.Attachments.Store(id, $"b{i}.txt", new MemoryStream(new byte[] { (byte)i }), "text/plain");
                    await s.SaveChangesAsync();
                }
                await WriteVersionsAsync(store, id, clusterWide: false, from: 1, count: 30);
                await PruneUsersRevisionsAsync(store, minToKeep: 10);

                foreach (var cv in await RevisionCvsAsync(store, id))
                {
                    if (await HasRevisionAttachmentAsync(store, id, cv))
                        withAttachment.Add(cv);
                }
                Assert.True(withAttachment.Count == 10, $"precondition: the 10 kept revisions expose the attachment on 6.2, {withAttachment.Count} do");
            }

            var maxTime = CleanupBatch.MaxTime;
            CleanupBatch.MaxTime = TimeSpan.FromMilliseconds(1);
            try
            {
                await UpgradeServerAsync("current", node);
                var stoppedOnASegment = await WatchForAttachmentResumeAsync(node.Url, db);
                await WaitForRevisionsKeyCleanupAsync(node.Url, db);

                using (var store = CreateStore(node.Url, db))
                {
                    var failures = new List<string>();
                    if (stoppedOnASegment == false)
                        failures.Add("precondition: no batch stopped inside the document's revision attachments");
                    foreach (var cv in withAttachment)
                    {
                        if (await HasRevisionAttachmentAsync(store, id, cv) == false)
                            failures.Add($"revision '{cv}' no longer exposes its attachment");
                    }
                    foreach (var row in RevisionAttachmentRowsUnder62Keys(node.Url, db, new[] { id }))
                        failures.Add($"left under a 6.2 key: {row}");

                    var database = await DatabaseAtAsync(node.Url, db);
                    if (KeyCleanerWalkStateLeft(database))
                        failures.Add("the cleanup left its walk state behind");

                    Assert.True(failures.Count == 0, string.Join("; ", failures));
                }
            }
            finally
            {
                CleanupBatch.MaxTime = maxTime;
            }
        }

        // A document's revision attachments that are already hashed: nothing moves, but passing them counts against the batch's time
        // limit, so a batch stops among them and the next one continues after the last row passed. Flow: a database created on 6.2 moves
        // to 7.2.6, where a document with an attachment gets 20,000 more versions (20,001 hashed revision attachment rows); upgraded in
        // place with a batch time limit of 1 ms.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62Via726_HashedAttachmentScanSpansBatches_EveryRevisionKeepsItsAttachment()
        {
            const string id = "users/with-attachment";
            var db = GetDatabaseName();
            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 1_000_000);
                await WriteVersionsAsync(store, "users/on-62", clusterWide: false, from: 0, count: 3);
            }

            await UpgradeServerAsync(Released72Version, node);
            List<string> newest;
            using (var store = CreateStore(node.Url, db))
            {
                await AddAttachmentAsync(store, id, createAs: "first");
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 20_000; i++)
                        await bulk.StoreAsync(new User { Name = "v" + i }, id);
                }

                newest = await RevisionCvsAsync(store, id);
                foreach (var cv in newest)
                    Assert.True(await HasRevisionAttachmentAsync(store, id, cv), $"precondition: revision '{cv}' exposes the attachment on 7.2.6");
            }

            var maxTime = CleanupBatch.MaxTime;
            CleanupBatch.MaxTime = TimeSpan.FromMilliseconds(1);
            try
            {
                await UpgradeServerAsync("current", node);
                var stoppedOnASegment = await WatchForAttachmentResumeAsync(node.Url, db);
                await WaitForRevisionsKeyCleanupAsync(node.Url, db);

                using (var store = CreateStore(node.Url, db))
                {
                    var failures = new List<string>();
                    if (stoppedOnASegment == false)
                        failures.Add("no batch stopped among the document's hashed revision attachments: one batch passed all of them");
                    foreach (var cv in newest)
                    {
                        if (await HasRevisionAttachmentAsync(store, id, cv) == false)
                            failures.Add($"revision '{cv}' no longer exposes its attachment");
                    }

                    var database = await DatabaseAtAsync(node.Url, db);
                    if (KeyCleanerWalkStateLeft(database))
                        failures.Add("the cleanup left its walk state behind");

                    Assert.True(failures.Count == 0, string.Join("; ", failures));
                }
            }
            finally
            {
                CleanupBatch.MaxTime = maxTime;
            }
        }

        // Import on a server created on 6.2 and upgraded in place. (1) Importing revisions a database already holds as 6.2 rows
        // must add nothing: on 6.2 a database gets regular, cluster-wide and attachment revisions and is exported; upgraded, it
        // imports that export. (2) Revision tombstones exported from A and imported into B, which got A's revisions by import on
        // 6.2, must delete the same revisions on B when A prunes after the upgrade.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Smuggler | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradedFrom62_ImportAddsNoTwins_ImportedRevisionTombstonesDeleteTheRevisions()
        {
            var db = GetDatabaseName();
            var dbA = GetDatabaseName() + "-a";
            var dbB = GetDatabaseName() + "-b";
            var ids = new[] { "users/regular", "users/cluster-tx", "users/with-attachment" };
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "export-62.ravendbdump");
            var revisions = DatabaseItemType.RevisionDocuments;
            // no database-record types: the 7.2 client's defaults include types a 6.2 server does not know
            var noRecord = DatabaseRecordItemType.None;

            var node = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            var before = new Dictionary<string, List<string>>();

            // ---- on 6.2 ----
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            using (var storeA = await CreatePersistentStoreAsync(node.Url, dbA))
            using (var storeB = await CreatePersistentStoreAsync(node.Url, dbB))
            {
                foreach (var s in new IDocumentStore[] { store, storeA, storeB })
                    await ConfigureUsersRevisionsAsync(s, minToKeep: 100);

                await WriteVersionsAsync(store, "users/regular", clusterWide: false, from: 0, count: 3);
                await WriteVersionsAsync(store, "users/cluster-tx", clusterWide: true, from: 0, count: 3);
                await AddAttachmentAsync(store, "users/with-attachment", createAs: "att");
                await WriteVersionsAsync(store, "users/with-attachment", clusterWide: false, from: 1, count: 2);
                foreach (var id in ids)
                    before[id] = await RevisionCvsAsync(store, id);

                await WriteVersionsAsync(storeA, "users/regular", clusterWide: false, from: 0, count: 3);
                await ExportImportAsync(node.Url, dbA, storeB, DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments);
            }

            // smuggler operations are tagged with the node, so this store keeps topology updates on (unlike CreateStore)
            using (var smugglerStore = new DocumentStore { Urls = new[] { node.Url }, Database = db }.Initialize())
            {
                var export = await smugglerStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = revisions, OperateOnDatabaseRecordTypes = noRecord }, file);
                await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            await UpgradeServerAsync("current", node);

            // ---- (1) import the 6.2 export ----
            using (var smugglerStore = new DocumentStore { Urls = new[] { node.Url }, Database = db }.Initialize())
            {
                var import = await smugglerStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = revisions, OperateOnDatabaseRecordTypes = noRecord }, file);
                await import.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = CreateStore(node.Url, db))
            {
                var twins = TwinRevisionRows(node.Url, db, ids);
                var failures = new List<string>();
                foreach (var id in ids)
                {
                    var after = await RevisionCvsAsync(store, id);
                    if (after.Count != before[id].Count)
                        failures.Add($"'{id}': {before[id].Count} revisions before the import, {after.Count} after");
                }
                Assert.True(twins.Count == 0 && failures.Count == 0, $"twins: {string.Join("; ", twins)}; {string.Join("; ", failures)}");
            }

            // ---- (2) A prunes; B imports A's revision tombstones ----
            using (var storeA = CreateStore(node.Url, dbA))
            using (var storeB = CreateStore(node.Url, dbB))
            {
                var onABefore = await RevisionCvsAsync(storeA, "users/regular");
                Assert.True(onABefore.All((await RevisionCvsAsync(storeB, "users/regular")).Contains), "precondition: B has every revision of A");

                await PruneUsersRevisionsAsync(storeA, minToKeep: 1);
                await ExportImportAsync(node.Url, dbA, storeB, DatabaseItemType.Tombstones);

                var onA = await RevisionCvsAsync(storeA, "users/regular");
                var onB = await RevisionCvsAsync(storeB, "users/regular");
                var left = onABefore.Where(cv => onA.Contains(cv) == false && onB.Contains(cv)).ToList();
                Assert.True(left.Count == 0, $"A deleted '{string.Join("', '", left)}', B still has them after importing A's tombstones");
            }
        }

        // Known limitation (red): 4.2.116+, 5.1.10+, 5.2.2+, 5.3 and 5.4 up to 5.4.109 delete a document's revisions count key after
        // a limited delete of its revisions (MinimumRevisionsToKeep = 0 with MaximumRevisionsToDeleteUponDocumentUpdate), so the
        // revisions left have no count entry. The cleanup walks only the documents in the count tree: the rows left stay under
        // their old keys and, once the storage is hash-only, cannot be found by change vector. Flow: on 5.4.109 a document gets 5
        // revisions; Users then keep 0 and delete at most 2 per update; one more update; the database is upgraded in place.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Interversion, RavenPlatform.Windows,
            Skip = "Known limitation: revisions left without a count entry by 4.2.116+, 5.1.10+, 5.2.2+, 5.3 and 5.4 up to 5.4.109 are not walked by the cleanup and stay under their old keys")]
        public async Task UpgradedFrom54_RevisionsLeftWithoutCount_MovedToHashedKeys()
        {
            const string id = "users/limited-delete";
            const string lastVersionThatDropsTheCount = "5.4.109";
            var db = GetDatabaseName();
            var node = await GetServerAsync(lastVersionThatDropsTheCount, customSettings: PersistentSettings);
            List<string> leftOn54;
            using (var store = await CreatePersistentStoreAsync(node.Url, db))
            {
                await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await WriteVersionsAsync(store, id, clusterWide: false, from: 0, count: 5);
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 100 },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 0, MaximumRevisionsToDeleteUponDocumentUpdate = 2 }
                    }
                }));
                await WriteVersionsAsync(store, id, clusterWide: false, from: 5, count: 1);

                leftOn54 = await RevisionCvsAsync(store, id);
                Assert.True(leftOn54.Count == 3, $"precondition: {lastVersionThatDropsTheCount} deleted 2 of the 5 revisions, it lists {leftOn54.Count}");
            }

            await UpgradeServerAsync("current", node);
            using (var store = CreateStore(node.Url, db))
            {
                await WaitForRevisionsKeyCleanupAsync(node.Url, db);

                var failures = new List<string>();
                foreach (var cv in RevisionRowsUnder62Keys(node.Url, db, id))
                    failures.Add($"revision '{cv}' is still under its old key");
                foreach (var cv in leftOn54)
                {
                    using var s = store.OpenAsyncSession();
                    if (await s.Advanced.Revisions.GetAsync<User>(cv) == null)
                        failures.Add($"revision '{cv}' is listed but not found by its change vector");
                }
                await CollectListedVsCountAsync(store, id, "after the cleanup", failures);
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // ---- servers created on 6.2 and upgraded in place, replicating throughout ----

        // A hub upgraded from 6.2 sends a cluster-wide revision's change vector in another form ("RAFT:..,TRXN:.." comes back as
        // "RAFT:.., TRXN:.."); the sink must not store it as a second row. Flow: a 6.2 hub and sink, both upgraded in place, then
        // the hub keeps writing.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_BothUpgradedFrom62_SinkStoresNoTwinRevisions()
        {
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };

            var hubNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            var sinkNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            // ---- on 6.2 ----
            using (var hubStore = await CreatePersistentStoreAsync(hubNode.Url, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkNode.Url, sinkDb))
            {
                await ConfigureRevisionsAsync(hubStore);
                await ConfigureRevisionsAsync(sinkStore);
                await SetupHubToSinkAsync(hubStore, sinkStore);

                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 3);

                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v2");
            }

            // ---- in-place upgrade of both servers ----
            await UpgradeServerAsync("current", sinkNode);
            await UpgradeServerAsync("current", hubNode);

            using (var hubStore = CreateStore(hubNode.Url, hubDb))
            using (var sinkStore = CreateStore(sinkNode.Url, sinkDb))
            {
                // the hub keeps writing (a sink does not prune replicated revisions, so no limit is set here)
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 3, count: 2);

                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v4");

                var twins = TwinRevisionRows(sinkNode.Url, sinkDb, docs.Select(d => d.Item1));
                Assert.True(twins.Count == 0, $"twin revision rows on the sink: {string.Join("; ", twins)}");
            }
        }

        // A new node's revision tombstones carry the other form; the old nodes, which do not prune replicated updates themselves,
        // must still delete their 6.2 rows. Flow: a 2-node 6.2 cluster is upgraded in place and gains a node; the Users limit is
        // lowered and the documents are updated on the new node. users/regular is the control.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task ClusterUpgradedFrom62_NewNodePrunes_OldNodesDeleteTheir62Revisions()
        {
            var db = GetDatabaseName();
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };

            var cluster = await CreateCluster(new[] { Server62Version, Server62Version }, customSettings: PersistentSettings);
            var (nodeA, nodeB) = (cluster[0], cluster[1]);

            // ---- on 6.2: 5 versions of each document, revisions kept ----
            using (var storeA = CreateStore(nodeA.Url, db))
            {
                await storeA.Maintenance.Server.SendAsync(new CreateDatabaseOperation(PersistentRecord(db), replicationFactor: 2));
                await ConfigureUsersRevisionsAsync(storeA, minToKeep: 100);
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(storeA, id, clusterWide, from: 0, count: 5);
            }
            using (var storeB = CreateStore(nodeB.Url, db))
            {
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(storeB, id, "v4");
            }

            // ---- in-place upgrade; a new node joins and gets the database ----
            await UpgradeServerAsync("current", nodeA);
            await UpgradeServerAsync("current", nodeB);

            var nodeC = GetNewServer(new ServerCreationOptions { RunInMemory = false });
            await AddClusterNodeAsync(new[] { nodeA.Url, nodeB.Url }, nodeC.WebUrl);

            var oldRevisions = new Dictionary<string, List<string>>();
            using (var storeA = CreateStore(nodeA.Url, db))
            using (var storeC = CreateStore(nodeC.WebUrl, db))
            {
                await storeA.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(db));
                foreach (var (id, _) in docs)
                {
                    await WaitForDocumentNameAsync(storeC, id, "v4", timeoutMs: 60_000);
                    oldRevisions[id] = await RevisionCvsAsync(storeA, id);
                }
                await WaitForRevisionCountAsync(nodeC.WebUrl, db, docs.Select(d => d.Item1), expected: 5);

                // ---- the new node prunes: lower limit, then regular updates on node C ----
                await ConfigureUsersRevisionsAsync(storeC, minToKeep: 2);
                foreach (var (id, _) in docs)
                    await WriteVersionsAsync(storeC, id, clusterWide: false, from: 5, count: 2);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(storeA, id, "v6", timeoutMs: 60_000);
                await WaitForRevisionCountAsync(nodeC.WebUrl, db, docs.Select(d => d.Item1), expected: 2);
                await WriteMarkerAndWaitAsync(storeC, storeA, "after-prune"); // written after the revision tombstones, so it reaches node A after them

            }

            var left = RevisionsWithIdentityOf(nodeA.Url, db, oldRevisions);
            Assert.True(left.Count == 0, $"revisions pruned on the new node are still on node A: {string.Join("; ", left)}");
            AssertRowsMatchCount(nodeA.Url, db, docs.Select(d => d.Item1));
        }

        // ---- a 6.2 hub cluster and sink; while the sink is down the hub task moves to a new hub node, which sends everything again ----

        // The production failure: the re-send leaves twins on the sink, and a conflict whose resolved put prunes a 6.2 row and its
        // twin threw on every batch, so replication stopped. Flow: while the sink is down for its upgrade, the hub cluster is
        // upgraded, gains a node and the task moves to it; the new node, unknown to the sink, sends everything again; a hub write
        // conflicts with a sink edit, resolved to latest, while Users keep 2 revisions (a collection setting, unlike Default,
        // applies to resolved revisions too).
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_NewHubNodeResendsToUpgradedSink_ConflictPrunes_SinkKeepsReplicating()
        {
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };
            var chain = await SetupHubClusterAndSinkOn62Async();
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 3);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v2");
            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain);

            var (sinkUrl, sinkDb) = (chain.Sink.Url, chain.SinkDb);
            using (var hubStoreC = CreateStore(chain.HubC, chain.HubDb))
            using (var sinkStore = CreateStore(sinkUrl, sinkDb))
            {
                // a local edit on the sink (cap still 100, no pruning yet)
                foreach (var (id, _) in docs)
                    await WriteVersionsAsync(sinkStore, id, clusterWide: false, from: 100, count: 1);

                // conflict on the sink (a regular write on the hub vs the sink's local edit), resolved to latest, with a low
                // cap so the resolved put prunes
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 2);
                await sinkStore.Maintenance.Server.SendAsync(new ModifyConflictSolverOperation(sinkDb, collectionByScript: null, resolveToLatest: true));
                foreach (var (id, _) in docs)
                    await WriteVersionsAsync(hubStoreC, id, clusterWide: false, from: 3, count: 1);

                var notConverged = await NotConvergedAsync(sinkStore, docs.Select(d => d.Item1), "v3");
                if (notConverged.Count > 0)
                    Assert.Fail($"the sink stopped accepting replication after the conflict on {string.Join(", ", notConverged)}: {await OutgoingReplicationErrorAsync(hubStoreC)}");
                AssertRowsMatchCount(sinkUrl, sinkDb, docs.Select(d => d.Item1));
            }
        }

        // Revisions the sink pruned on 6.2 have tombstones keyed by the old form; the new hub node sends them again in the other
        // form, and they must not come back. The revisions the sink kept must then go when the hub prunes them. Flow: on 6.2 the
        // hub writes 5 versions and the sink keeps 2; the new hub node's re-send; then the hub lowers the Users limit to 2 and
        // updates its documents twice. users/regular is the control (its form does not change). The tombstone cleaner first runs
        // 5 minutes after load, so the 6.2 tombstones survive the upgrade.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_NewHubNodeResendsToUpgradedSink_PrunedOn62StayDeleted_HubPrunesTheRest()
        {
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };
            var ids = docs.Select(d => d.Item1).ToArray();
            var chain = await SetupHubClusterAndSinkOn62Async();
            var pruned = new Dictionary<string, List<string>>();
            // ---- on 6.2: replicate 5 versions, then the sink prunes to 2 ----
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 5);
                foreach (var id in ids)
                    await WaitForDocumentNameAsync(sinkStore, id, "v4");

                var before = new Dictionary<string, List<string>>();
                foreach (var id in ids)
                    before[id] = await RevisionCvsAsync(sinkStore, id);

                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 2);

                foreach (var id in ids)
                {
                    pruned[id] = before[id].Except(await RevisionCvsAsync(sinkStore, id)).ToList();
                    Assert.True(pruned[id].Count == 3, $"'{id}': expected the 6.2 sink to prune 3 of 5 revisions, pruned {pruned[id].Count}");
                }
            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain);

            var (hubC, hubDb, sinkUrl, sinkDb) = (chain.HubC, chain.HubDb, chain.Sink.Url, chain.SinkDb);
            var back = RevisionsWithIdentityOf(sinkUrl, sinkDb, pruned);
            Assert.True(back.Count == 0, $"revisions pruned on 6.2 came back on the sink: {string.Join("; ", back)}");
            AssertRowsMatchCount(sinkUrl, sinkDb, ids);

            // ---- the hub prunes under normal retention: lower limit, then regular updates on the hub node ----
            var keptOnSink = new Dictionary<string, List<string>>();
            using (var hubStoreC = CreateStore(hubC, hubDb))
            using (var sinkStore = CreateStore(sinkUrl, sinkDb))
            {
                foreach (var id in ids)
                    keptOnSink[id] = await RevisionCvsAsync(sinkStore, id);

                await ConfigureUsersRevisionsAsync(hubStoreC, minToKeep: 2);
                foreach (var id in ids)
                    await WriteVersionsAsync(hubStoreC, id, clusterWide: false, from: 5, count: 2);
                await WaitForRevisionCountAsync(hubC, hubDb, ids, expected: 2);
                foreach (var id in ids)
                    await WaitForDocumentNameAsync(sinkStore, id, "v6", timeoutMs: 60_000);
                await WriteMarkerAndWaitAsync(hubStoreC, sinkStore, "after-hub-prune"); // written after the revision tombstones, so it reaches the sink after them
            }

            AssertRowsMatchCount(sinkUrl, sinkDb, ids);
            var left = RevisionsWithIdentityOf(sinkUrl, sinkDb, keptOnSink);
            Assert.True(left.Count == 0, $"revisions pruned on the hub are still on the sink: {string.Join("; ", left)}");
        }

        // A hand-built revision (an import appended ",TRXN:…") that has an attachment gets a twin from the re-send, with its
        // attachments under the hashed key; when the sink prunes its 6.2 rows, the kept revisions must keep their attachments.
        // Flow: on 6.2 the hub writes a cluster-wide transaction, adds an attachment, exports, edits and imports the older export;
        // while the sink is down the hub is upgraded, gains a node and the task moves to it; then the sink prunes. A SINK tag
        // never gets here: an upgraded sink that knows the database id restores the tag on receipt.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_NewHubNodeResendsToUpgradedSink_ImportedRevisionWithAttachment_SinkPrunes_RevisionsKeepAttachments()
        {
            const string id = "users/imported";
            var chain = await SetupHubClusterAndSinkOn62Async(hubOnNodeAOnly: true);
            var (hubA, hubDb, sinkNode, sinkDb) = (chain.HubA, chain.HubDb, chain.Sink, chain.SinkDb);
            // ---- on 6.2 ----
            using (var hubStore = CreateStore(hubA.Url, hubDb))
            using (var sinkStore = CreateStore(sinkNode.Url, sinkDb))
            {
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 1);
                await AddAttachmentAsync(hubStore, id);
                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 1, count: 1);
                await ImportOlderExportAsync(hubA.Url, hubDb, hubStore, id, nextVersion: 2, DatabaseItemType.Documents | DatabaseItemType.Attachments);
                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 3, count: 1);
                await WaitForDocumentNameAsync(sinkStore, id, "v3", timeoutMs: 60_000);

            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain);
            var twins = TwinRevisionRows(sinkNode.Url, sinkDb, new[] { id });

            using (var sinkStore = CreateStore(sinkNode.Url, sinkDb))
            {
                // which revisions (by identity) carry the attachment, before the prune
                var before = await RevisionCvsAsync(sinkStore, id);
                var hadAttachment = new List<string>();
                foreach (var cv in before)
                {
                    var has = await HasRevisionAttachmentAsync(sinkStore, id, cv);
                    if (has)
                        hadAttachment.Add(cv);
                }
                Assert.True(twins.Count == 0, $"the re-send created {twins.Count} twin(s) of 6.2 rows on the sink");

                // ---- the sink prunes its oldest 6.2 rows, keeping 2 revisions ----
                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 2);

                var failures = new List<string>();
                var kept = await RevisionCvsAsync(sinkStore, id);
                Assert.Equal(2, kept.Count);
                foreach (var cv in kept)
                {
                    var expected = RevisionsOfSameIdentity(hadAttachment, cv).Count > 0;
                    var has = await HasRevisionAttachmentAsync(sinkStore, id, cv);
                    if (expected && has == false)
                        failures.Add($"revision '{cv}' lost its attachment when its 6.2 copy was pruned");
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // A 6.2 sink matches by exact text, so a re-send in another form adds a second row for each kept revision and brings
        // pruned ones back. Flow: a 6.2 hub cluster and sink; the sink prunes; while it is down the hub is upgraded, gains a node
        // and the task moves to it; the sink comes back still on 6.2. users/regular is the control.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_HubUpgradedFirst_Sink62_ResendAddsNoTwinsAndNothingComesBack()
        {
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };
            var chain = await SetupHubClusterAndSinkOn62Async();
            var kept = new Dictionary<string, List<string>>();
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 5);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v4");

                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 2);
                foreach (var (id, _) in docs)
                {
                    kept[id] = await RevisionCvsAsync(sinkStore, id);
                }
            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain, sinkVersion: Server62Version);

            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                var failures = new List<string>();
                foreach (var (id, _) in docs)
                {
                    var now = await RevisionCvsAsync(sinkStore, id);
                    if (now.Count != kept[id].Count)
                        failures.Add($"'{id}': the 6.2 sink kept {kept[id].Count} revisions, has {now.Count} after the re-send");
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // The production failure caused by a hub on this build: its re-send to a 7.2.6 sink must add no rows, and the sink must
        // still delete old revisions. Flow: a 6.2 hub cluster and sink; while the sink is down it moves to 7.2.6 and the hub to
        // current with a new node that takes the task; the sink then writes with a lower cap. users/regular is the control.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_HubOnCurrent_SinkOn726_ResendAddsNoRows_OldRevisionsStillDeleted()
        {
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };
            var chain = await SetupHubClusterAndSinkOn62Async();
            var before = new Dictionary<string, List<string>>();
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 5);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v4");
                foreach (var (id, _) in docs)
                {
                    before[id] = await RevisionCvsAsync(sinkStore, id);
                }
            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain, sinkVersion: Released72Version);

            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                var failures = new List<string>();
                foreach (var (id, _) in docs)
                {
                    var now = await RevisionCvsAsync(sinkStore, id);
                    if (now.Count != before[id].Count)
                        failures.Add($"'{id}': the 7.2.6 sink had {before[id].Count} revisions, has {now.Count} after the re-send");
                }

                // ---- the 7.2.6 sink deletes old revisions ----
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 2);
                foreach (var (id, _) in docs)
                {
                    try
                    {
                        await WriteVersionsAsync(sinkStore, id, clusterWide: false, from: 100, count: 1);
                    }
                    catch (Exception e)
                    {
                        failures.Add($"'{id}': a write on the 7.2.6 sink failed while deleting old revisions: {e.Message.Split('\n')[0]}");
                        continue;
                    }
                    var kept = await RevisionCvsAsync(sinkStore, id);
                    if (kept.Count != 2)
                        failures.Add($"'{id}': the 7.2.6 sink keeps {kept.Count} revisions after deleting old ones, expected 2: '{string.Join("', '", kept)}'");
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // ---- a 6.2 sink fed by a hub on this build ----

        // A 6.2 sink matches revisions by exact text, so a hub that sends a revision in one form and its tombstone in another
        // leaves the revision there. Flow: a hub born on current pulls to a 6.2 sink, then deletes its revisions. users/regular
        // is the control.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task PullReplication_Sink62_HubDeletesRevisions_SinkDeletesThem()
        {
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            var docs = new[] { ("users/regular", false), ("users/cluster-tx", true) };

            var hubUrl = GetNewServer(new ServerCreationOptions { RunInMemory = false }).WebUrl;
            var sinkNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            using (var hubStore = await CreatePersistentStoreAsync(hubUrl, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkNode.Url, sinkDb))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupHubToSinkAsync(hubStore, sinkStore);

                foreach (var (id, clusterWide) in docs)
                    await WriteVersionsAsync(hubStore, id, clusterWide, from: 0, count: 5);
                foreach (var (id, _) in docs)
                    await WaitForDocumentNameAsync(sinkStore, id, "v4");


                // the hub deletes 3 of 5 revisions per document; its revision tombstones replicate to the sink
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 2);

                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-delete");

                var mismatches = new List<string>();
                foreach (var (id, _) in docs)
                {
                    var onHub = await RevisionCvsAsync(hubStore, id);
                    var onSink = await RevisionCvsAsync(sinkStore, id);
                    if (onSink.Count != onHub.Count)
                        mismatches.Add($"'{id}': hub has {onHub.Count} revisions, sink has {onSink.Count}");
                }

                Assert.True(mismatches.Count == 0, $"revisions deleted on the hub are still on the 6.2 sink: {string.Join("; ", mismatches)}");
            }
        }

        // Importing an older export over a document whose change vector has a TRXN entry appends ",TRXN:…" by hand (no space,
        // not sorted); the 6.2 sink must still delete that revision when the hub does. Flow: a hub on current pulls to a 6.2 sink;
        // the hub imports an older export, then deletes the revision. clusterTxInHistory=false is the baseline (nothing appended).
        [RavenMultiplatformTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task PullReplication_Sink62_ImportedOlderVersion_SinkDeletesItsRevision(bool clusterTxInHistory)
        {
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            const string id = "users/imported";

            var hubServer = GetNewServer(new ServerCreationOptions { RunInMemory = false });
            var sinkNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            using (var hubStore = await CreatePersistentStoreAsync(hubServer.WebUrl, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkNode.Url, sinkDb))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupHubToSinkAsync(hubStore, sinkStore);

                await WriteVersionsAsync(hubStore, id, clusterWide: clusterTxInHistory, from: 0, count: 2);
                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 2, count: 1);

                await ImportOlderExportAsync(hubServer.WebUrl, hubDb, hubStore, id, nextVersion: 3);

                string underTest;
                using (var s = hubStore.OpenAsyncSession())
                {
                    var doc = await s.LoadAsync<User>(id);
                }
                underTest = (await RevisionCvsAsync(hubStore, id)).First();

                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 10, count: 1); // newer, so the imported version can be pruned
                await WaitForDocumentNameAsync(sinkStore, id, "v10", timeoutMs: 60_000);

                // the hub keeps only the newest revision; its revision tombstones replicate
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 1);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-delete");

                var onHub = await RevisionCvsAsync(hubStore, id);
                var onSink = await RevisionCvsAsync(sinkStore, id);

                Assert.DoesNotContain(underTest, onHub);
                var stillOnSink = RevisionsOfSameIdentity(onSink, underTest);
                Assert.True(stillOnSink.Count == 0, $"the hub deleted '{underTest}', the 6.2 sink still has it as '{string.Join("', '", stillOnSink)}'");
            }
        }

        // A hub drops unused database ids from its change vector; a sender to a 6.2 sink that retags the entries it does not know
        // as SINK, while keys that embed the change vector (revision tombstone ids, revision attachment keys) keep the original,
        // makes the 6.2 sink miss attachments and deletes, and its own purge leave orphan revision attachments. Flow: data
        // reaches the hub by external replication from a database later retired and declared unused; the hub keeps 2 revisions;
        // then the sink keeps 1 (measured by the public statistics: CountOfAttachments counts revision attachments on 6.2.9, the
        // document's and the kept revision's = 2). markUnused=false is the baseline.
        [RavenMultiplatformTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task PullReplication_Sink62_UnusedDatabaseId_RevisionsAndAttachmentsStayConsistent(bool markUnused)
        {
            var srcDb = GetDatabaseName() + "-src";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            const string regular = "users/regular", withAttachment = "users/with-attachment";

            var hubServer = GetNewServer(new ServerCreationOptions { RunInMemory = false });
            var sinkNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            using (var srcStore = await CreatePersistentStoreAsync(hubServer.WebUrl, srcDb))
            using (var hubStore = await CreatePersistentStoreAsync(hubServer.WebUrl, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkNode.Url, sinkDb))
            {
                foreach (var store in new IDocumentStore[] { srcStore, hubStore, sinkStore })
                    await ConfigureUsersRevisionsAsync(store, minToKeep: 100);

                // ---- data arrives at the hub from another database ----
                await ReplicateToAsync(srcStore, hubDb, hubServer.WebUrl);

                await WriteVersionsAsync(srcStore, regular, clusterWide: false, from: 0, count: 5);
                await AddAttachmentAsync(srcStore, withAttachment, createAs: "att");
                await WriteVersionsAsync(srcStore, withAttachment, clusterWide: false, from: 1, count: 2);
                await WaitForDocumentNameAsync(hubStore, regular, "v4", timeoutMs: 60_000);
                await WaitForDocumentNameAsync(hubStore, withAttachment, "v2", timeoutMs: 60_000);

                // ---- the source database is retired and its id declared unused on the hub ----
                await RetireSourceDatabaseAsync(srcStore, hubStore, markUnused);

                // ---- first transfer to the 6.2 sink ----
                await SetupHubToSinkAsync(hubStore, sinkStore);
                await WaitForDocumentNameAsync(sinkStore, regular, "v4", timeoutMs: 60_000);
                await WaitForDocumentNameAsync(sinkStore, withAttachment, "v2", timeoutMs: 60_000);
                await WaitForDocumentNameAsync(sinkStore, "sync/hub-local", "x", timeoutMs: 60_000);

                // revision attachments: a sink revision exposes the attachment iff the hub's copy of that revision does
                // (both lists are newest first)
                var failures = new List<string>();
                var hubCvs = await RevisionCvsAsync(hubStore, withAttachment);
                var sinkCvs = await RevisionCvsAsync(sinkStore, withAttachment);
                for (var i = 0; i < Math.Min(hubCvs.Count, sinkCvs.Count); i++)
                {
                    var onHub = await HasRevisionAttachmentAsync(hubStore, withAttachment, hubCvs[i]);
                    var onSink = await HasRevisionAttachmentAsync(sinkStore, withAttachment, sinkCvs[i]);
                    if (onHub && onSink == false)
                        failures.Add($"sink revision '{sinkCvs[i]}' of '{withAttachment}' lost its attachment");
                }

                // ---- the hub deletes revisions; its revision tombstones replicate ----
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 2);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-delete");

                foreach (var id in new[] { regular, withAttachment })
                {
                    var onHub = await RevisionCvsAsync(hubStore, id);
                    var onSink = await RevisionCvsAsync(sinkStore, id);
                    if (onSink.Count != onHub.Count)
                        failures.Add($"'{id}': hub has {onHub.Count} revisions, sink has {onSink.Count}");
                }

                // ---- the 6.2 sink purges its own revisions, keeping the newest ----
                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 1);
                var after = await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation());
                if (after.CountOfAttachments != 2)
                    failures.Add($"after the sink purged to 1 revision, expected 2 attachment rows (document + kept revision), got {after.CountOfAttachments}");

                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // A document from a retired database the hub declared unused, edited on a 6.2 sink and sent back, must keep that database
        // id under its own node tag on the hub, not "SINK".
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task TwoWayPull_Sink62_UnusedDatabaseId_ComesBackUnderItsOwnTag()
        {
            var srcDb = GetDatabaseName() + "-src";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            const string id = "users/from-retired";

            var pair = await GetSecuredPairOn62Async();
            await UpgradeServerAsync("current", pair.Hub, customSettings: pair.Settings);

            using (var srcStore = await CreatePersistentStoreAsync(pair.Hub.Url, srcDb, pair.Admin))
            using (var hubStore = await CreatePersistentStoreAsync(pair.Hub.Url, hubDb, pair.Admin))
            using (var sinkStore = await CreatePersistentStoreAsync(pair.Sink.Url, sinkDb, pair.Admin))
            {
                foreach (var store in new IDocumentStore[] { srcStore, hubStore, sinkStore })
                    await ConfigureUsersRevisionsAsync(store, minToKeep: 100);

                await ReplicateToAsync(srcStore, hubDb, pair.Hub.Url);
                await WriteVersionsAsync(srcStore, id, clusterWide: false, from: 0, count: 2);
                await WaitForDocumentNameAsync(hubStore, id, "v1", timeoutMs: 60_000);

                var srcDbId = await RetireSourceDatabaseAsync(srcStore, hubStore);

                await SetupTwoWayPullAsync(hubStore, sinkStore, pair.Hub.Url, pair.Pull);
                await WaitForDocumentNameAsync(sinkStore, id, "v1", timeoutMs: 60_000);

                await WriteVersionsAsync(sinkStore, id, clusterWide: false, from: 2, count: 1);
                await WaitForDocumentNameAsync(hubStore, id, "v2", timeoutMs: 60_000);

                var onHub = await DocumentChangeVectorAsync(hubStore, id);
                var entry = onHub.Split(',').Select(e => e.Trim()).FirstOrDefault(e => e.EndsWith("-" + srcDbId, StringComparison.Ordinal));
                Assert.True(entry != null, $"precondition: the hub's change vector '{onHub}' has an entry for the retired database {srcDbId}");
                Assert.False(entry.StartsWith("SINK:", StringComparison.Ordinal),
                    $"the hub's change vector carries the unused database id under SINK after the round trip: '{onHub}'");
            }
        }

        // ---- two-way pull on 6.2: the sink's own writes come back from the hub ----

        // In two-way pull on 6.2, a cluster-wide revision written on the sink comes back from the hub rebuilt ("RAFT:x-g,
        // TRXN:y-t") and the sink stores it as a second row (revisions are always sent back). After the sink's upgrade every
        // revision must be listed once, and the echo must add no new twins; then the 6.2 hub's revision tombstones must delete
        // both copies of each pruned revision, with one tombstone per revision. Flow: see SetupTwoWayEchoTwinsOnCurrentSinkAsync;
        // the upgraded sink keeps writing; then the hub keeps only its newest revision. users/regular is the control.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task TwoWayPull62_EchoTwins_SinkOnCurrent_EachRevisionListedOnce_HubPrunes_SinkDeletesEveryCopyOnce()
        {
            var echo = await SetupTwoWayEchoTwinsOnCurrentSinkAsync();
            using (var hubStore = CreateStore(echo.HubUrl, echo.HubDb, echo.Certificate))
            using (var sinkStore = CreateStore(echo.SinkUrl, echo.SinkDb, echo.Certificate))
            {
                // ---- the sink keeps writing; the echo continues ----
                foreach (var id in echo.Ids)
                    await WriteVersionsAsync(sinkStore, id, clusterWide: id == "users/cluster-tx", from: 3, count: 2);
                foreach (var id in echo.Ids)
                    await WaitForDocumentNameAsync(hubStore, id, "v4", timeoutMs: 60_000);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "echo-barrier-on-current");
                await WaitForRevisionsKeyCleanupAsync(echo.SinkUrl, echo.SinkDb);

                var failures = new List<string>();
                foreach (var id in echo.Ids)
                {
                    var listed = await RevisionCvsAsync(sinkStore, id);
                    if (listed.Count != 5)
                        failures.Add($"'{id}': 5 versions written on the sink, it lists {listed.Count} revisions");
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));
                AssertRowsMatchCount(echo.SinkUrl, echo.SinkDb, echo.Ids);

                // ---- the hub keeps only its newest revision ----
                var hubBefore = new Dictionary<string, List<string>>();
                foreach (var id in echo.Ids)
                    hubBefore[id] = await RevisionCvsAsync(hubStore, id);
                // counted for these documents only: the prune also deletes old revisions of the marker document (a User)
                var tombstonesBefore = CountRevisionTombstones(echo.SinkUrl, echo.SinkDb, echo.Ids);

                await PruneUsersRevisionsAsync(hubStore, minToKeep: 1);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-prune");

                // the sink's background cleanup deletes the 6.2 copies the hub's SINK-tagged tombstones missed
                var pruned = await AssertPrunedRevisionsGoneAsync(hubStore, sinkStore, echo.Ids, hubBefore, waitForCleanup: TimeSpan.FromMinutes(2));
                var tombstonesAdded = CountRevisionTombstones(echo.SinkUrl, echo.SinkDb, echo.Ids) - tombstonesBefore;
                Assert.True(tombstonesAdded == pruned, $"the sink added {tombstonesAdded} tombstones for {pruned} pruned revisions, expected one per revision");
                await AssertListedRevisionsMatchCountAsync(sinkStore, echo.Ids);
            }
        }

        // As TwoWayPull62_EchoTwins_SinkOnCurrent_EachRevisionListedOnce_HubPrunes_SinkDeletesEveryCopyOnce, by import: the hub's
        // revision tombstones (stored with SINK-tagged change vectors) are exported and imported into the sink.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Smuggler | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task TwoWayPull62_EchoTwins_SinkOnCurrent_ImportsHubTombstones_DeletesEveryCopy()
        {
            var echo = await SetupTwoWayEchoTwinsOnCurrentSinkAsync();
            using (var hubStore = CreateStore(echo.HubUrl, echo.HubDb, echo.Certificate))
            using (var sinkStore = CreateStore(echo.SinkUrl, echo.SinkDb, echo.Certificate))
            {
                await RemoveSinkPullTaskAsync(sinkStore);

                var hubBefore = new Dictionary<string, List<string>>();
                foreach (var id in echo.Ids)
                    hubBefore[id] = await RevisionCvsAsync(hubStore, id);
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 1);

                await ExportImportAsync(echo.HubUrl, echo.HubDb, sinkStore, DatabaseItemType.Tombstones, echo.Certificate);

                await AssertPrunedRevisionsGoneAsync(hubStore, sinkStore, echo.Ids, hubBefore);
                await AssertListedRevisionsMatchCountAsync(sinkStore, echo.Ids);
            }
        }

        // The hub stores revisions written on the sink with the sink's database id tagged SINK ("SINK:3-x"), and its revision
        // tombstones carry that form; the sink holds them as "A:3-x" and must still delete them. Flow: a secured 6.2 pair in
        // two-way pull, the sink writes; the sink is upgraded in place; the hub keeps only its newest revision. The sink matches
        // them through its cleanup (hashed keys ignore tags), so it is given time.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task TwoWayPull_SinkUpgradedFrom62_HubPrunesSinkWrittenRevisions_SinkDeletesThem()
        {
            const string id = "users/regular";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            var pair = await GetSecuredPairOn62Async();

            using (var hubStore = await CreatePersistentStoreAsync(pair.Hub.Url, hubDb, pair.Admin))
            using (var sinkStore = await CreatePersistentStoreAsync(pair.Sink.Url, sinkDb, pair.Admin))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupTwoWayPullAsync(hubStore, sinkStore, pair.Hub.Url, pair.Pull);
                await WriteVersionsAsync(sinkStore, id, clusterWide: false, from: 0, count: 3);
                await WaitForDocumentNameAsync(hubStore, id, "v2", timeoutMs: 60_000);
            }

            await UpgradeServerAsync("current", pair.Sink, customSettings: pair.Settings);

            using (var hubStore = CreateStore(pair.Hub.Url, hubDb, pair.Admin))
            using (var sinkStore = CreateStore(pair.Sink.Url, sinkDb, pair.Admin))
            {
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "sink-upgraded");

                var failures = await HubPrunesAndSinkMissesAsync(hubStore, sinkStore, id, minToKeep: 1, "current sink",
                    waitForCleanup: TimeSpan.FromMinutes(2));
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // ---- the affected sink: 6.2 rows plus the twins a 7.2.6 hub's re-send left (see SetupAffectedSinkAsync) ----

        // The re-send left a second row for revisions still alive. After the cleanup each revision is listed once, under the change
        // vector it had on 6.2 (the one its 6.2 peers hold), and as many as on the hub; the hub's revision tombstones then delete
        // both copies (6.2 row and twin) of each revision it prunes; and deleting all revisions of a document (it threw) leaves none
        // and the count at 0.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_EachRevisionListedOnce_HubPrunesAndDeleteAllDeleteEveryCopy()
        {
            var affected = await SetupAffectedSinkAsync();
            using (var hubStoreC = CreateStore(affected.HubC.Url, affected.HubDb))
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                // ---- each revision is listed once, under its 6.2 change vector, as many as on the hub ----
                var failures = await WaitForEachRevisionListedOnceAsync(sinkStore, affected.ListedOn62, timeoutMs: 120_000);
                var hubBefore = new Dictionary<string, List<string>>();
                foreach (var id in affected.Ids)
                {
                    hubBefore[id] = await RevisionCvsAsync(hubStoreC, id);
                    var onSink = await RevisionCvsAsync(sinkStore, id);
                    if (onSink.Count != hubBefore[id].Count)
                        failures.Add($"'{id}': hub lists {hubBefore[id].Count} revisions, the sink lists {onSink.Count}");
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));

                // ---- the hub keeps only its newest revision; its revision tombstones delete every copy of the others ----
                await PruneUsersRevisionsAsync(hubStoreC, minToKeep: 1);
                await WriteMarkerAndWaitAsync(hubStoreC, sinkStore, "after-prune");
                await AssertPrunedRevisionsGoneAsync(hubStoreC, sinkStore, affected.Ids, hubBefore);
                AssertRowsMatchCount(affected.Sink.Url, affected.SinkDb, affected.Ids);

                // ---- deleting all revisions of each document deletes every row once ----
                foreach (var id in affected.Ids)
                    await sinkStore.Maintenance.SendAsync(new DeleteRevisionsOperation(id));
                foreach (var id in affected.Ids)
                    Assert.Empty(await RevisionCvsAsync(sinkStore, id));
                AssertRowsMatchCount(affected.Sink.Url, affected.SinkDb, affected.Ids);
            }
        }

        // The production path: a hub write conflicts with a sink edit and is resolved to latest; the cap of 2 makes the resolved
        // put prune 6.2 rows and twins. The sink must keep replicating, with right counts.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_ConflictResolutionPrunes_SinkKeepsReplicating()
        {
            var affected = await SetupAffectedSinkAsync();
            using (var hubStoreC = CreateStore(affected.HubC.Url, affected.HubDb))
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                foreach (var id in affected.Ids)
                    await WriteVersionsAsync(sinkStore, id, clusterWide: false, from: 100, count: 1); // a local edit on the sink

                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 2);
                await sinkStore.Maintenance.Server.SendAsync(new ModifyConflictSolverOperation(affected.SinkDb, collectionByScript: null, resolveToLatest: true));
                foreach (var id in affected.Ids)
                    await WriteVersionsAsync(hubStoreC, id, clusterWide: false, from: 3, count: 1);

                var notConverged = await NotConvergedAsync(sinkStore, affected.Ids, "v3");
                if (notConverged.Count > 0)
                    Assert.Fail($"the sink stopped accepting replication after the conflict on {string.Join(", ", notConverged)}: {await OutgoingReplicationErrorAsync(hubStoreC)}");
                AssertRowsMatchCount(affected.Sink.Url, affected.SinkDb, affected.Ids);
            }
        }

        // The hub's revision tombstones, exported and imported instead of replicated, must delete both copies of each pruned
        // revision. Flow: the sink's pull task is removed and the hub keeps only its newest revision.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Smuggler | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_ImportsHubRevisionTombstones_DeletesEveryCopy()
        {
            var affected = await SetupAffectedSinkAsync();
            using (var hubStoreC = CreateStore(affected.HubC.Url, affected.HubDb))
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                await RemoveSinkPullTaskAsync(sinkStore);

                var hubBefore = new Dictionary<string, List<string>>();
                foreach (var id in affected.Ids)
                    hubBefore[id] = await RevisionCvsAsync(hubStoreC, id);
                await PruneUsersRevisionsAsync(hubStoreC, minToKeep: 1);

                // the export may run on any hub node: wait until every node has the tombstones the prune created on node C
                using (var hubStoreA = CreateStore(affected.HubA.Url, affected.HubDb))
                using (var hubStoreB = CreateStore(affected.HubB.Url, affected.HubDb))
                {
                    await WriteMarkerAndWaitAsync(hubStoreC, hubStoreA, "after-prune");
                    await WaitForDocumentNameAsync(hubStoreB, "sync/marker", "after-prune", timeoutMs: 60_000);
                }
                await ExportImportAsync(affected.HubC.Url, affected.HubDb, sinkStore, DatabaseItemType.Tombstones);

                await AssertPrunedRevisionsGoneAsync(hubStoreC, sinkStore, affected.Ids, hubBefore);
                await AssertListedRevisionsMatchCountAsync(sinkStore, affected.Ids);
            }
        }

        // Deleting old revisions planned "rows - keep" deletes, oldest first, and each 6.2 row it deleted took its twin with it,
        // so it ran into the newest revision. Document users/mixed: two cluster-wide revisions (6.2 rows with twins), then a
        // regular one stored once. A capped update of it must keep its newest revision; then enforcing the configuration runs the
        // same delete over every document: it must complete, keep each document's newest revision, and leave every count right.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_DeleteOldRevisionsAndEnforceConfiguration_KeepTheNewestRevision()
        {
            var affected = await SetupAffectedSinkAsync(withMixedDocument: true);
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                // ---- a capped update deletes the old revisions of users/mixed ----
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 1);
                await WriteVersionsAsync(sinkStore, MixedId, clusterWide: false, from: 100, count: 1);

                var newestMixed = await DocumentChangeVectorAsync(sinkStore, MixedId);
                var keptMixed = await RevisionCvsAsync(sinkStore, MixedId);
                Assert.True(keptMixed.Contains(newestMixed), $"the newest revision '{newestMixed}' of '{MixedId}' was deleted; kept: '{string.Join("', '", keptMixed)}'");

                // ---- enforcing the configuration deletes the old revisions of every document ----
                var newest = new Dictionary<string, string>();
                foreach (var id in affected.Ids)
                    newest[id] = await DocumentChangeVectorAsync(sinkStore, id);

                var failures = new List<string>();
                try
                {
                    var enforce = await sinkStore.Operations.SendAsync(new EnforceRevisionsConfigurationOperation());
                    await enforce.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                }
                catch (Exception e)
                {
                    failures.Add($"enforcing the revisions configuration failed: {e.Message.Split('\n')[0]}");
                }

                foreach (var id in affected.Ids)
                {
                    var kept = await RevisionCvsAsync(sinkStore, id);
                    if (kept.Contains(newest[id]) == false)
                        failures.Add($"the newest revision '{newest[id]}' of '{id}' was deleted; kept '{string.Join("', '", kept)}'");
                    await CollectListedVsCountAsync(sinkStore, id, "after enforcing the configuration", failures);
                }
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // A revision imported on 6.2 over a cluster-wide document carries an attachment and got a twin from the re-send. After the
        // cleanup every revision attachment row is under its hashed key; when the sink keeps 2 revisions, and again when a conflict
        // resolved to latest prunes, the kept revisions must still expose the attachment, the sink must keep replicating, and
        // deleting the revisions leaves no revision attachment.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_ImportedRevisionWithAttachment_SinkAndConflictPrune_KeptRevisionsKeepTheAttachment()
        {
            var affected = await SetupAffectedSinkAsync(withImportedAttachment: true);
            using (var hubStoreC = CreateStore(affected.HubC.Url, affected.HubDb))
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                var hadAttachment = await RevisionsWithAttachmentAsync(sinkStore, ImportedId);
                Assert.True(hadAttachment.Count > 0, "precondition: revisions of the imported document expose the attachment on the sink");
                var under62Keys = RevisionAttachmentRowsUnder62Keys(affected.Sink.Url, affected.SinkDb, affected.Ids);
                Assert.True(under62Keys.Count == 0, $"after the cleanup every revision attachment row is under its hashed key: {string.Join("; ", under62Keys)}");

                // ---- the sink keeps 2 revisions ----
                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 2);
                var kept = await RevisionCvsAsync(sinkStore, ImportedId);
                Assert.True(kept.Count == 2, $"the sink kept {kept.Count} revisions of '{ImportedId}', expected 2: '{string.Join("', '", kept)}'");
                var failures = new List<string>();
                foreach (var cv in kept)
                {
                    if (RevisionsOfSameIdentity(hadAttachment, cv).Count > 0 && await HasRevisionAttachmentAsync(sinkStore, ImportedId, cv) == false)
                        failures.Add($"revision '{cv}' lost its attachment when the sink pruned");
                }
                await AssertListedRevisionsMatchCountAsync(sinkStore, new[] { ImportedId });

                // ---- a hub write conflicts with a sink edit, resolved to latest; Users still keep 2 ----
                await WriteVersionsAsync(sinkStore, ImportedId, clusterWide: false, from: 100, count: 1);
                await sinkStore.Maintenance.Server.SendAsync(new ModifyConflictSolverOperation(affected.SinkDb, collectionByScript: null, resolveToLatest: true));
                await WriteVersionsAsync(hubStoreC, ImportedId, clusterWide: false, from: 4, count: 1);
                await WaitForDocumentNameAsync(sinkStore, ImportedId, "v4", timeoutMs: 60_000);

                foreach (var cv in await RevisionCvsAsync(sinkStore, ImportedId))
                {
                    if (RevisionsOfSameIdentity(hadAttachment, cv).Count > 0 && await HasRevisionAttachmentAsync(sinkStore, ImportedId, cv) == false)
                        failures.Add($"revision '{cv}' lost its attachment in the conflict resolution");
                }
                await AssertListedRevisionsMatchCountAsync(sinkStore, new[] { ImportedId });

                // ---- deleting the revisions leaves only the document's own attachment ----
                await sinkStore.Maintenance.SendAsync(new DeleteRevisionsOperation(ImportedId));
                Assert.Empty(await RevisionCvsAsync(sinkStore, ImportedId));
                var stats = await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation());
                if (stats.CountOfAttachments != 1)
                    failures.Add($"after deleting the revisions, {stats.CountOfAttachments} attachments remain, expected only the document's own");
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // A twin of an imported revision (",TRXN:…" appended by hand) on a sink that kept only its newest 3 revisions, none of
        // them cluster-wide: it must be listed once, nothing pruned on 6.2 may come back, the count must be right and the document
        // prunable. deleted: the hub also deleted the document.
        [RavenMultiplatformTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AffectedSink_ImportedRevisionTwin_NoClusterWideRevisionLeft_ListedOnceAndPrunable(bool deleted)
        {
            var affected = await SetupAffectedSinkAsync(importedWithoutClusterWideRevision: true, deleteImported: deleted);
            using (var hubStoreC = CreateStore(affected.HubC.Url, affected.HubDb))
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                var failures = new List<string>();
                var onHub = await RevisionCvsAsync(hubStoreC, ImportedId);
                var onSink = await RevisionCvsAsync(sinkStore, ImportedId);

                foreach (var cv in affected.ImportedOnSinkOn62)
                {
                    var copies = RevisionsOfSameIdentity(onSink, cv);
                    if (copies.Count != 1)
                        failures.Add($"revision '{cv}' is listed {copies.Count} times on the sink: '{string.Join("', '", copies)}'");
                }
                var cameBack = onSink.Where(cv => RevisionsOfSameIdentity(affected.ImportedOnSinkOn62, cv).Count == 0).ToList();
                if (cameBack.Count > 0)
                    failures.Add($"revisions the sink pruned on 6.2 came back: '{string.Join("', '", cameBack)}'");
                await CollectListedVsCountAsync(sinkStore, ImportedId, "before pruning", failures);

                try
                {
                    await PruneUsersRevisionsAsync(sinkStore, minToKeep: 1);
                }
                catch (Exception e)
                {
                    failures.Add($"pruning on the sink failed: {e.Message.Split('\n')[0]}");
                }
                await CollectListedVsCountAsync(sinkStore, ImportedId, "after pruning", failures);

                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // Known limitation (red): on 7.2.6 a conflict rewrote a revision held as a 6.2 row and a twin into one row but kept the
        // count, one too high; the cleanup does not recount, so it stays wrong on current.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows,
            Skip = "Known limitation: a 7.2.6 conflict merged a 6.2 row and its twin into one row but kept the count one too high; the cleanup does not recount. " +
                   "Not fixed: a recount means another pass over every revision, too costly for a case that needs a 7.2.6 conflict over a 6.2 row with a twin")]
        public async Task AffectedSink_ConflictOn726_CountRightOnCurrent()
        {
            const string id = ImportedId;
            var affected = await SetupAffectedSinkAsync(withImportedAttachment: true, importIsLastWrite: true, onSink726: async x =>
            {
                using var sinkStore = CreateStore(x.Sink.Url, x.SinkDb);
                var newest = await DocumentChangeVectorAsync(sinkStore, id);
                var copies = RevisionsOfSameIdentity(await RevisionCvsAsync(sinkStore, id), newest);
                Assert.True(copies.Count == 2 && copies.Contains(newest),
                    $"precondition: the 7.2.6 sink holds its document's revision '{newest}' as a 6.2 row and a twin; holds '{string.Join("', '", copies)}'");

                await CauseConflictOnSinkAsync(x.Sink.Url, x.SinkDb, id, "other");
                await AssertResolvedAConflictAsync(sinkStore, id);
            });

            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
                await AssertListedRevisionsMatchCountAsync(sinkStore, new[] { id });
        }

        // When the sink's cleanup merges a 6.2 row and its twin, the merged row keeps the 6.2 row's etag and the twin is deleted
        // without a tombstone. A database the sink replicates to, which never got the twin, must still get the twin's flags.
        // Flow: on 6.2.9 a 2-node hub cluster pulls to the sink, which replicates on to a database of its own server (D). The hub
        // writes the document by cluster-wide transactions, then regular writes and an import, so its newest revision is in the
        // import form (",TRXN:…" appended, not the form a 7.2.6 hub rebuilds). The sink -> D task is disabled; the hub and the sink
        // move to 7.2.6 and the hub resolves a conflict, so it sends that revision again, rebuilt and flagged Conflicted. The sink
        // (and D) are upgraded to current, the cleanup merges the sink's two rows, and the task is enabled again.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSink_ReplicatesOnward_DownstreamGetsTheMergedFlags()
        {
            const string id = MixedId;
            var chain = await SetupHubClusterAndSinkOn62Async();
            var downstreamDb = chain.SinkDb + "-downstream";
            long toDownstream;
            string merged;

            // ---- on 6.2: the sink replicates on to D; both hold the document's revisions ----
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            using (var downstreamStore = await CreatePersistentStoreAsync(chain.Sink.Url, downstreamDb))
            {
                await ConfigureUsersRevisionsAsync(downstreamStore, minToKeep: 100);
                toDownstream = await ReplicateToAsync(sinkStore, downstreamDb, chain.Sink.Url);

                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 2);
                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 2, count: 1);
                await ImportOlderExportAsync(chain.HubA.Url, chain.HubDb, hubStore, id, nextVersion: 3);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "before-upgrade");
                await WriteMarkerAndWaitAsync(sinkStore, downstreamStore, "before-upgrade-downstream");

                merged = await DocumentChangeVectorAsync(hubStore, id);
                Assert.True(merged != SortedChangeVector(merged), $"precondition: the newest revision '{merged}' is not in the form a 7.2.6 hub rebuilds");
                Assert.True(RevisionsOfSameIdentity(await RevisionCvsAsync(downstreamStore, id), merged).Count == 1, $"precondition: D holds '{merged}'");

                await sinkStore.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(toDownstream, OngoingTaskType.Replication, disable: true));
            }

            // ---- the hub and the sink move to 7.2.6; the hub resolves a conflict and sends its newest revision again ----
            await UpgradeServerAsync(Released72Version, chain.HubA);
            await UpgradeServerAsync(Released72Version, chain.HubB);
            await UpgradeServerAsync(Released72Version, chain.Sink);
            await CauseConflictOnHubAsync(new[] { chain.HubA.Url, chain.HubB.Url }, chain.HubDb, id);

            using (var hubStore = CreateStore(chain.HubB.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            using (var downstreamStore = CreateStore(chain.Sink.Url, downstreamDb))
            {
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-conflict");

                var onSink = await RevisionFlagsAsync(sinkStore, id);
                var onDownstream = await RevisionFlagsAsync(downstreamStore, id);

                var copies = RevisionsOfSameIdentity(onSink.Keys, merged);
                Assert.True(copies.Count == 2 && copies.Any(cv => onSink[cv].Contains("Conflicted")),
                    $"precondition: the 7.2.6 sink holds '{merged}' as a 6.2 row and a twin flagged Conflicted; it holds '{string.Join("', '", copies)}'");
                Assert.False(onDownstream[RevisionsOfSameIdentity(onDownstream.Keys, merged)[0]].Contains("Conflicted"), $"precondition: D's '{merged}' is not flagged Conflicted yet");
            }

            // ---- the sink (and D) move to current; the cleanup merges the two rows; the task to D is enabled again ----
            await UpgradeServerAsync("current", chain.Sink);
            await WaitForRevisionsKeyCleanupAsync(chain.Sink.Url, chain.SinkDb);
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            using (var downstreamStore = CreateStore(chain.Sink.Url, downstreamDb))
            {
                var onSink = await RevisionFlagsAsync(sinkStore, id);
                var copies = RevisionsOfSameIdentity(onSink.Keys, merged);
                Assert.True(copies.Count == 1 && onSink[copies[0]].Contains("Conflicted"),
                    $"precondition: the cleanup merged '{merged}' into one row flagged Conflicted; the sink holds '{string.Join("', '", copies)}'");

                await sinkStore.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(toDownstream, OngoingTaskType.Replication, disable: false));
                await WriteMarkerAndWaitAsync(sinkStore, downstreamStore, "after-merge");

                var onDownstream = await RevisionFlagsAsync(downstreamStore, id);
                var onD = RevisionsOfSameIdentity(onDownstream.Keys, merged);
                Assert.True(onD.Count > 0 && onD.All(cv => onDownstream[cv].Contains("Conflicted")),
                    $"D's '{string.Join("', '", onD)}' is not flagged Conflicted after the sink's merge: {string.Join(" | ", onD.Select(cv => onDownstream[cv]))}");
            }
        }

        // The sink comes back already on current while the hub, on 7.2.6, sends every revision again rebuilt. Revisions the sink
        // deleted on 6.2 must stay deleted: those it pruned, and those of a document deleted in a cluster-wide transaction whose
        // revisions it deleted (the delete revision included). The others must not get a second row.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSinkOnCurrent_726HubResends_RevisionsDeletedOn62StayDeleted()
        {
            var affected = await SetupAffectedSinkAsync(sinkReceivesOnCurrent: true, sinkPrunesOn62: true, sinkPurgesDeletedDocOn62: true);
            Assert.True(affected.Twins.Count == 0, $"the current sink stored twins of its 6.2 rows: {string.Join("; ", affected.Twins)}");
            var back = RevisionsWithIdentityOf(affected.Sink.Url, affected.SinkDb, affected.PrunedOn62);
            Assert.True(back.Count == 0, $"revisions pruned on 6.2 came back on the sink: {string.Join("; ", back)}");
            AssertRowsMatchCount(affected.Sink.Url, affected.SinkDb, affected.Ids);

            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                var onSink = await RevisionCvsAsync(sinkStore, DeletedId);
                var purgedBack = affected.PurgedOn62.SelectMany(cv => RevisionsOfSameIdentity(onSink, cv)).Distinct().ToList();
                Assert.True(purgedBack.Count == 0, $"revisions of '{DeletedId}' deleted on the 6.2 sink came back: '{string.Join("', '", purgedBack)}'");
                await AssertListedRevisionsMatchCountAsync(sinkStore, affected.Ids);
            }
        }

        // Revision tombstones must be kept until the cleanup has used them: here the sink's tombstone cleaner (every minute) runs
        // while its pull task is re-created, before the 7.2.6 hub's new node sends everything again. Revisions pruned on 6.2 must
        // stay deleted.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task AffectedSinkOnCurrent_TombstoneCleanerRunsBeforeTheResend_RevisionsPrunedOn62StayDeleted()
        {
            var affected = await SetupAffectedSinkAsync(sinkReceivesOnCurrent: true, sinkPrunesOn62: true, purgeTombstonesBeforeReSend: true);
            using (var sinkStore = CreateStore(affected.Sink.Url, affected.SinkDb))
            {
                var back = await WaitForNoneListedAsync(sinkStore, affected.PrunedOn62, timeoutMs: 120_000);
                Assert.True(back.Count == 0, $"revisions pruned on 6.2 came back on the sink: {string.Join("; ", back)}");
            }
        }

        // Known limitation (red): a force-created revision the sink deleted on 6.2 comes back when the 7.2.6 hub sends it again,
        // and the cleanup keeps it: its stored flags cannot tell it from one force-created on purpose after the delete. Flow: the
        // sink comes back on current and the 7.2.6 hub sends everything again; on 6.2 the document is written by cluster-wide
        // transactions with revisions on (force-creating a revision for a cluster-wide document that has none throws on 6.2.9 and
        // 7.2.x), then with them off; its revision is force-created and the sink deletes it.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows,
            Skip = "Known limitation: a force-created revision deleted on the 6.2 sink comes back when the 7.2.6 hub resends it; its flags cannot tell it from one force-created on purpose")]
        public async Task AffectedSinkOnCurrent_726HubResendsForceCreatedRevisionDeletedOn62_StaysDeleted()
        {
            const string id = "users/force-created";
            var chain = await SetupHubClusterAndSinkOn62Async();
            var (hubDb, sinkDb, sink) = (chain.HubDb, chain.SinkDb, chain.Sink);
            string deletedOnSink;
            // ---- on 6.2 ----
            using (var hubStore = CreateStore(chain.HubA.Url, hubDb))
            using (var sinkStore = CreateStore(sink.Url, sinkDb))
            {
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 1);
                await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration> { ["Users"] = new() { Disabled = true } }
                }));
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 1, count: 1);
                using (var s = hubStore.OpenAsyncSession())
                {
                    s.Advanced.Revisions.ForceRevisionCreationFor(id);
                    await s.SaveChangesAsync();
                }
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "force-created");

                var onSink = await RevisionFlagsAsync(sinkStore, id);
                var forceCreated = onSink.Where(r => r.Value.Contains("ForceCreated")).Select(r => r.Key).ToList();
                Assert.True(forceCreated.Count == 1, $"precondition: the 6.2 sink holds one force-created revision of '{id}'");
                deletedOnSink = forceCreated[0];

                await sinkStore.Maintenance.SendAsync(new DeleteRevisionsOperation(id, removeForceCreatedRevisions: true));
                Assert.True((await RevisionCvsAsync(sinkStore, id)).Count == 0, "precondition: the 6.2 sink deleted the revision");
            }

            await MoveHubTaskToNewNodeWhileSinkIsDownAsync(chain, hubVersion: Released72Version);

            using (var sinkStore = CreateStore(sink.Url, sinkDb))
            {
                // the sink's cleanup deletes the revision that came back
                var back = await WaitForNoneListedAsync(sinkStore, new Dictionary<string, List<string>> { [id] = new() { deletedOnSink } }, timeoutMs: 120_000);
                Assert.True(back.Count == 0, $"the force-created revision deleted on the 6.2 sink came back and stayed: {string.Join("; ", back)}");
            }
        }

        // ---- a 6.2 sink fed by a 7.2.6 hub ----

        // A 7.2.5/7.2.6 hub sends revisions of a retired (unused) database as "SINK:…" but their attachments under "A:…", so
        // the 6.2 sink's purge leaves orphan attachments next to revision tombstones; after the sink's upgrade the cleanup must
        // remove them (the document's attachment and the kept revision's = 2). This build's hub sends both under one change
        // vector, so the hub here is 7.2.6.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task Hub726UnusedDatabaseId_Sink62Purges_SinkUpgraded_NoOrphanRevisionAttachments()
        {
            var srcDb = GetDatabaseName() + "-src";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            const string id = "users/with-attachment";

            var hub = await GetServerAsync(Released72Version, customSettings: PersistentSettings);
            var sinkNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);

            using (var srcStore = await CreatePersistentStoreAsync(hub.Url, srcDb))
            using (var hubStore = await CreatePersistentStoreAsync(hub.Url, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkNode.Url, sinkDb))
            {
                foreach (var store in new IDocumentStore[] { srcStore, hubStore, sinkStore })
                    await ConfigureUsersRevisionsAsync(store, minToKeep: 100);

                await ReplicateToAsync(srcStore, hubDb, hub.Url);
                await AddAttachmentAsync(srcStore, id, createAs: "att");
                await WriteVersionsAsync(srcStore, id, clusterWide: false, from: 1, count: 2);
                await WaitForDocumentNameAsync(hubStore, id, "v2", timeoutMs: 60_000);
                await RetireSourceDatabaseAsync(srcStore, hubStore);

                await SetupHubToSinkAsync(hubStore, sinkStore);
                await WaitForDocumentNameAsync(sinkStore, id, "v2", timeoutMs: 60_000);
                await WaitForDocumentNameAsync(sinkStore, "sync/hub-local", "x", timeoutMs: 60_000);

                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 1);
                var on62 = await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.True(on62.CountOfAttachments > 2, $"precondition: the 6.2 purge left orphan revision attachments, CountOfAttachments={on62.CountOfAttachments}");
            }

            await UpgradeServerAsync("current", sinkNode);
            using (var sinkStore = CreateStore(sinkNode.Url, sinkDb))
            {
                var sw = Stopwatch.StartNew();
                long attachments;
                do
                {
                    attachments = (await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfAttachments;
                    if (attachments == 2)
                        break;
                    await Task.Delay(1000);
                } while (sw.Elapsed < TimeSpan.FromMinutes(2));
                Assert.True(attachments == 2, $"on current, expected 2 attachment rows (document + kept revision), got {attachments}");
            }
        }

        // Known limitation (red): a 6.2 sink fed by a 7.2.6 hub stores the hub's revision tombstones without deleting its
        // revisions (other form); once its tombstone cleaner purges them on 6.2, nothing on the sink records the delete. Flow: a
        // 7.2.6 hub pulls to a 6.2 sink and keeps 2 of 5 revisions; the sink's pull task is removed, its tombstone cleaner (every
        // minute) runs, and the sink is upgraded.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows,
            Skip = "Known limitation: once the 6.2 sink's tombstone cleaner purged the 7.2.6 hub's revision tombstones, nothing on the sink records those deletes")]
        public async Task Hub726DeletesRevisions_Sink62PurgesTheTombstones_SinkDeletesThemAfterItsUpgrade()
        {
            const string id = "users/cluster-tx";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            var hub = await GetServerAsync(Released72Version, customSettings: PersistentSettings);
            var sink = await GetServerAsync(Server62Version, customSettings: new Dictionary<string, string>(PersistentSettings)
            {
                [RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = "1"
            });

            List<string> deletedOnHub;
            using (var hubStore = await CreatePersistentStoreAsync(hub.Url, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sink.Url, sinkDb))
            {
                await ConfigureUsersRevisionsAsync(hubStore, minToKeep: 100);
                await ConfigureUsersRevisionsAsync(sinkStore, minToKeep: 100);
                await SetupHubToSinkAsync(hubStore, sinkStore);
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 5);
                await WaitForDocumentNameAsync(sinkStore, id, "v4", timeoutMs: 60_000);

                var hubBefore = await RevisionCvsAsync(hubStore, id);
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 2);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-delete");

                var onHub = await RevisionCvsAsync(hubStore, id);
                var onSink = await RevisionCvsAsync(sinkStore, id);
                deletedOnHub = hubBefore.Where(cv => RevisionsOfSameIdentity(onHub, cv).Count == 0).ToList();
                Assert.True(deletedOnHub.Count == 3 && deletedOnHub.All(cv => RevisionsOfSameIdentity(onSink, cv).Count == 1),
                    "precondition: the hub deleted 3 revisions and the 6.2 sink still has each of them");

                await RemoveSinkPullTaskAsync(sinkStore);
                var sw = Stopwatch.StartNew();
                long tombstones;
                while ((tombstones = (await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfTombstones) > 0 &&
                       sw.Elapsed < TimeSpan.FromMinutes(3))
                    await Task.Delay(TimeSpan.FromSeconds(5));
            }

            await UpgradeServerAsync("current", sink);
            using (var sinkStore = CreateStore(sink.Url, sinkDb))
            {
                // the sink's cleanup would delete them
                var left = await WaitForNoneListedAsync(sinkStore, new Dictionary<string, List<string>> { [id] = deletedOnHub }, timeoutMs: 120_000);
                Assert.True(left.Count == 0, $"revisions the hub deleted are still on the sink: {string.Join("; ", left)}");
            }
        }

        // Known limitation (red): orphan revision attachments whose revision tombstones were purged on 6.2 stay; without a
        // tombstone an orphan cannot be told from an attachment whose revision is still on its way. Flow: as
        // Hub726UnusedDatabaseId_Sink62Purges_SinkUpgraded_NoOrphanRevisionAttachments, with the sink's tombstone cleaner run
        // before its upgrade.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows,
            Skip = "Known limitation: orphan revision attachments whose revision tombstones were purged on 6.2 cannot be told from attachments whose revision is still on its way")]
        public async Task Hub726UnusedDatabaseId_Sink62PurgesTheTombstones_NoOrphanAttachmentsAfterItsUpgrade()
        {
            const string id = "users/with-attachment";
            var srcDb = GetDatabaseName() + "-src";
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            var hub = await GetServerAsync(Released72Version, customSettings: PersistentSettings);
            var sink = await GetServerAsync(Server62Version, customSettings: new Dictionary<string, string>(PersistentSettings)
            {
                [RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval)] = "1"
            });

            using (var srcStore = await CreatePersistentStoreAsync(hub.Url, srcDb))
            using (var hubStore = await CreatePersistentStoreAsync(hub.Url, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sink.Url, sinkDb))
            {
                foreach (var store in new IDocumentStore[] { srcStore, hubStore, sinkStore })
                    await ConfigureUsersRevisionsAsync(store, minToKeep: 100);

                await ReplicateToAsync(srcStore, hubDb, hub.Url);
                await AddAttachmentAsync(srcStore, id, createAs: "att");
                await WriteVersionsAsync(srcStore, id, clusterWide: false, from: 1, count: 2);
                await WaitForDocumentNameAsync(hubStore, id, "v2", timeoutMs: 60_000);
                await RetireSourceDatabaseAsync(srcStore, hubStore);

                await SetupHubToSinkAsync(hubStore, sinkStore);
                await WaitForDocumentNameAsync(sinkStore, id, "v2", timeoutMs: 60_000);
                await WaitForDocumentNameAsync(sinkStore, "sync/hub-local", "x", timeoutMs: 60_000);

                await PruneUsersRevisionsAsync(sinkStore, minToKeep: 1);
                var stats = await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.True(stats.CountOfAttachments > 2, "precondition: the 6.2 sink kept orphan revision attachments (more than the document's and the kept revision's)");

                await RemoveSinkPullTaskAsync(sinkStore);
                var sw = Stopwatch.StartNew();
                long tombstones;
                while ((tombstones = (await sinkStore.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfTombstones) > 0 &&
                       sw.Elapsed < TimeSpan.FromMinutes(3))
                    await Task.Delay(TimeSpan.FromSeconds(5));
            }

            await UpgradeServerAsync("current", sink);
            // the sink's cleanup would delete them
            var orphans = OrphanRevisionAttachments(sink.Url, sinkDb, id);
            var sw2 = Stopwatch.StartNew();
            while (orphans.Count > 0 && sw2.Elapsed < TimeSpan.FromMinutes(2))
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                orphans = OrphanRevisionAttachments(sink.Url, sinkDb, id);
            }

            Assert.True(orphans.Count == 0, $"{orphans.Count} orphan revision attachments are still on the sink");
        }

        // A 6.2 sink fed by a 7.2.6 hub stores cluster-wide revisions in the rebuilt form; the hub on current deletes them in its
        // stored form, which the sink does not match while it runs 6.2 (known limitation, asserted as a precondition below). Its
        // revisions key cleanup applies those deletes once it is upgraded: it matches the hub's revision tombstones by hashed key.
        // After that the hub's deletes reach it directly. Flow: see SetupSink62FedBy726HubOnCurrentAsync; the hub keeps 2
        // revisions while the sink runs 6.2; the sink is upgraded in place; the hub keeps 1.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task Sink62FedBy726Hub_HubOnCurrentDeletes_SinkDeletesThemAfterItsUpgrade()
        {
            const string id = "users/cluster-tx";
            var chain = await SetupSink62FedBy726HubOnCurrentAsync(id);

            // ---- the hub keeps 2 revisions; the sink, still on 6.2, misses the deletes ----
            List<string> prunedWhileOn62;
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                var hubBefore = await RevisionCvsAsync(hubStore, id);
                await PruneUsersRevisionsAsync(hubStore, minToKeep: 2);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-prune-on-6.2");

                var onHub = await RevisionCvsAsync(hubStore, id);
                var onSink = await RevisionCvsAsync(sinkStore, id);
                prunedWhileOn62 = hubBefore.Where(cv => RevisionsOfSameIdentity(onHub, cv).Count == 0).ToList();
                Assert.True(prunedWhileOn62.Count > 0, $"precondition: the hub pruned revisions of '{id}'");
                // a single-entry change vector ("RAFT:1-…") has one form, so the 6.2 sink deletes it; it misses the rebuilt ones
                Assert.True(prunedWhileOn62.Any(cv => RevisionsOfSameIdentity(onSink, cv).Count > 0),
                    $"precondition: the 6.2 sink missed some of the hub's deletes; the hub pruned '{string.Join("', '", prunedWhileOn62)}', the sink lists '{string.Join("', '", onSink)}'");
            }

            // ---- the sink is upgraded in place to current; its revisions key cleanup applies the deletes it missed ----
            await UpgradeServerAsync("current", chain.Sink);
            await WaitForRevisionsKeyCleanupAsync(chain.Sink.Url, chain.SinkDb);
            using (var hubStore = CreateStore(chain.HubA.Url, chain.HubDb))
            using (var sinkStore = CreateStore(chain.Sink.Url, chain.SinkDb))
            {
                var left = await WaitForNoneListedAsync(sinkStore, new Dictionary<string, List<string>> { [id] = prunedWhileOn62 }, timeoutMs: 120_000);
                Assert.True(left.Count == 0, $"revisions the hub deleted while the sink ran 6.2 are still on the upgraded sink: {string.Join("; ", left)}");

                // ---- the hub keeps 1 revision; the upgraded sink deletes it directly ----
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "sink-upgraded");
                var failures = await HubPrunesAndSinkMissesAsync(hubStore, sinkStore, id, minToKeep: 1, "current sink");
                Assert.True(failures.Count == 0, string.Join("; ", failures));
            }
        }

        // ---- a sink born on this build, fed by a 6.2 hub that moves to 7.2.6 ----

        // A 7.2.6 hub that resolves a conflict sends its newest revision again, flagged Conflicted and rebuilt (sorted, ", "); a
        // sink born on current must hold it once, flagged, with the right count. Flow: on 6.2 the document gets cluster-wide,
        // regular and imported (",TRXN:…") versions and is pulled; the hub is upgraded to 7.2.6 and resolves a conflict. A 6.2
        // peer of the sink gets a second copy (the row is rewritten under the change vector received); upgrading the peer repairs
        // it.
        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task SinkBornOnCurrent_726HubResendsARevisionFlaggedConflicted_SinkHoldsItOnceFlagged()
        {
            var hubDb = GetDatabaseName() + "-hub";
            var sinkDb = GetDatabaseName() + "-sink";
            const string id = MixedId;

            var hubNode = await GetServerAsync(Server62Version, customSettings: PersistentSettings);
            var sinkServer = GetNewServer(new ServerCreationOptions { RunInMemory = false });

            string underTest;
            using (var hubStore = await CreatePersistentStoreAsync(hubNode.Url, hubDb))
            using (var sinkStore = await CreatePersistentStoreAsync(sinkServer.WebUrl, sinkDb))
            {
                foreach (var store in new IDocumentStore[] { hubStore, sinkStore })
                    await ConfigureUsersRevisionsAsync(store, minToKeep: 100);
                await SetupHubToSinkAsync(hubStore, sinkStore);

                // a hand-built form: importing an older export over a document with a TRXN entry appends ",TRXN:…"
                await WriteVersionsAsync(hubStore, id, clusterWide: true, from: 0, count: 2);
                await WriteVersionsAsync(hubStore, id, clusterWide: false, from: 2, count: 1);
                await ImportOlderExportAsync(hubNode.Url, hubDb, hubStore, id, nextVersion: 3);
                await WaitForDocumentNameAsync(sinkStore, id, "v2", timeoutMs: 60_000);
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "before-upgrade");

                underTest = await DocumentChangeVectorAsync(hubStore, id);
                var onSink = await RevisionCvsAsync(sinkStore, id);
                Assert.True(underTest != SortedChangeVector(underTest), $"precondition: the newest revision '{underTest}' is not in the form a 7.2.6 hub rebuilds");
                Assert.True(RevisionsOfSameIdentity(onSink, underTest).SequenceEqual(new[] { underTest }), $"precondition: the sink holds '{underTest}' once, as the hub wrote it");
            }

            // ---- the hub is upgraded in place to 7.2.6 and resolves a conflict on the document ----
            await UpgradeServerAsync(Released72Version, hubNode);
            await CauseConflictOnHubAsync(new[] { hubNode.Url }, hubDb, id);

            using (var hubStore = CreateStore(hubNode.Url, hubDb))
            using (var sinkStore = CreateStore(sinkServer.WebUrl, sinkDb))
            {
                await WriteMarkerAndWaitAsync(hubStore, sinkStore, "after-conflict");

                var flags = await RevisionFlagsAsync(sinkStore, id);

                var copies = RevisionsOfSameIdentity(flags.Keys, underTest);
                Assert.True(copies.Count == 1, $"the sink holds '{underTest}' {copies.Count} times: '{string.Join("', '", copies)}'");
                Assert.True(flags[copies[0]].Contains("Conflicted"), $"the sink's '{copies[0]}' is not flagged Conflicted: {flags[copies[0]]}");
                await AssertListedRevisionsMatchCountAsync(sinkStore, new[] { id });
            }
        }
    }
}
