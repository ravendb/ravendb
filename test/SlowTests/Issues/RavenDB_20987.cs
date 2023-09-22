using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20987 : ReplicationTestBase
    {
        public RavenDB_20987(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationOfRevisionTombstoneWithIdThatRequireEscaping(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1);

                var id = "users~shiran\r\n1";

                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "shiran";
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id }
                }));

                await EnsureReplicatingAsync(store1, store2);

                await CheckData(store2, options.DatabaseMode, id, expectedRevisionTombstones: 2);
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationOfAttachmentTombstoneWithIdThatRequireEscaping(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1);

                var id = "users~shiran\r\n1";

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    session.Advanced.Attachments.Store(id, "profile.png", profileStream);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id }
                }));

                await EnsureReplicatingAsync(store1, store2);

                await CheckData(store2, options.DatabaseMode, id, expectedRevisionTombstones: 2, expectedAttachmentTombstones: 1);
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ImportRevisionTombstoneWithIdThatRequireEscaping(Options options)
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(options))
                using (var store2 = GetDocumentStore(options))
                {
                    await RevisionsHelper.SetupRevisionsAsync(store1);

                    var id = "users~shiran\r\n1";

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), id);
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(id);
                        user.Name = "shiran";
                        await session.SaveChangesAsync();
                    }

                    await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                    {
                        DocumentIds = new[] { id }
                    }));

                    await CheckData(store1, options.DatabaseMode, id, expectedRevisionTombstones: 2);

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones
                    };
                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var importOptions = new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones
                    };
                    var importOperation = await store2.Smuggler.ImportAsync(importOptions, file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    await CheckData(store2, options.DatabaseMode, id, expectedRevisionTombstones: 2);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport | RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ImportAttachmentRevisionTombstoneWithIdThatRequireEscaping(Options options)
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(options))
                using (var store2 = GetDocumentStore(options))
                {
                    await RevisionsHelper.SetupRevisionsAsync(store1);

                    var id = "users~shiran\r\n1";

                    using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), id);
                        session.Advanced.Attachments.Store(id, "profile.png", profileStream);
                        await session.SaveChangesAsync();
                    }

                    await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                    {
                        DocumentIds = new[] { id }
                    }));

                    await CheckData(store1, options.DatabaseMode, id, expectedRevisionTombstones: 2, expectedAttachmentTombstones: 1);

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones
                    };
                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var importOptions = new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones
                    };
                    var importOperation = await store2.Smuggler.ImportAsync(importOptions, file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    await CheckData(store2, options.DatabaseMode, id, expectedRevisionTombstones: 2, expectedAttachmentTombstones: 1);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task CheckData(IDocumentStore store, RavenDatabaseMode mode, string id, long expectedRevisionTombstones, long expectedAttachmentTombstones = 0)
        {
            var documentDatabase = await GetDocumentDatabaseInstanceForAsync(store, mode, id);
            using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var totalExpected = expectedRevisionTombstones + expectedAttachmentTombstones;
                var tombstones = documentDatabase.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                Assert.Equal(totalExpected, tombstones.Count);

                var revisionTombstones = 0;
                var attachmentTombstones = 0;
                foreach (var tombstone in tombstones)
                {
                    switch (tombstone.Type)
                    {
                        case Tombstone.TombstoneType.Revision:
                            revisionTombstones++;
                            break;
                        case Tombstone.TombstoneType.Attachment:
                            attachmentTombstones++;
                            break;
                    }
                }

                Assert.Equal(expectedRevisionTombstones, revisionTombstones);
                Assert.Equal(expectedAttachmentTombstones, attachmentTombstones);
            }
        }
    }
}
