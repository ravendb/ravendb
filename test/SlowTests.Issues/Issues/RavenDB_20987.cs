using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
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

                await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new List<string>() { id }));

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

                await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new List<string>() { id }));

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

                    await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new List<string>() { id }));

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

                    await store1.Maintenance.SendAsync(new DeleteRevisionsOperation(new List<string>() { id }));

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

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ReplicateRevisionTombstones(Options options)
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            
            options.ReplicationFactor = 3;
            options.ModifyDatabaseName = _ => "foo";
            options.ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true };

            var o1 = options.Clone();
            o1.Server = cluster.Nodes[0];

            var o2 = options.Clone();
            o2.Server = cluster.Nodes[1];
            o2.CreateDatabase = false;

            var o3 = options.Clone();
            o3.Server = cluster.Nodes[2];
            o3.CreateDatabase = false;

            var id = $"users/1/{new string('x', 450)}";

            using (var store1 = GetDocumentStore(o1))
            using (var store2 = GetDocumentStore(o2))
            using (var store3 = GetDocumentStore(o3))
            using (var cts = new CancellationTokenSource())
            {
                var t1 = Run(store1, id, cts.Token);
                var t2 = Run(store2, id, cts.Token);
                var t3 = Run(store3, id, cts.Token);
                
                try
                {
                    await WaitForValueAsync(async () =>
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            var r = await session.Advanced.Revisions.GetCountForAsync(id);
                            return r >= 1000;
                        }
                    }, true);

                    await store1.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(store1.Database,
                        new RevisionsCollectionConfiguration { MaximumRevisionsToDeleteUponDocumentUpdate = 100, MinimumRevisionsToKeep = 10 }));

                    await WaitForValueAsync(async () =>
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            var r = await session.Advanced.Revisions.GetCountForAsync(id);
                            return r <= 12;
                        }
                    }, true);
                }
                finally
                {
                    cts.Cancel();
                }

                await Task.WhenAll(t1, t2, t3);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store3);
                await EnsureReplicatingAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store3);
                await EnsureReplicatingAsync(store3, store2);
                await EnsureReplicatingAsync(store3, store1);
            }
        }

        private static Task Run(DocumentStore store, string id, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                while (cancellationToken.IsCancellationRequested == false)
                {
                    try
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User { Name = Guid.NewGuid().ToString() }, id, cancellationToken);
                            await session.SaveChangesAsync(cancellationToken);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            });
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
