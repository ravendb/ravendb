using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ShardedPeriodicBackupTests : RavenTestBase
    {
        public ShardedPeriodicBackupTests(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupSharded(Options options)
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png"
            };
            try
            {
                var backupPath = NewDataPath(suffix: $"{options.DatabaseMode}_BackupFolder");

                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" }))
                using (var store2 = Sharding.GetDocumentStore())
                using (var store3 = GetDocumentStore(options))
                {
                    await Sharding.Backup.InsertData(store1, names);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions


                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store2);

                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store2, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // import

                    var dirs = Directory.GetDirectories(backupPath);

                    Assert.Equal(3, dirs.Length);
                    var importOptions = new DatabaseSmugglerImportOptions();

                    foreach (var dir in dirs)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(importOptions, dir);
                        importOptions.OperateOnTypes &= ~DatabaseSmugglerOptions.OperateOnFirstShardOnly;
                    }
                    
                    await Sharding.Backup.CheckData(store3, names, options.DatabaseMode);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupShardedIncremental()
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");

            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var shardNumToDocIds = new Dictionary<int, List<string>>();
                var dbRecord = await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store1.Database));
                var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);

                // generate data on store1, keep track of doc-ids per shard
                using (var session = store1.OpenAsyncSession())
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var user = new User { Name = i.ToString() };
                        var id = $"users/{i}";

                        var shardNumber = shardedCtx.GetShardNumber(context, id);
                        if (shardNumToDocIds.TryGetValue(shardNumber, out var ids) == false)
                        {
                            shardNumToDocIds[shardNumber] = ids = new List<string>();
                        }
                        ids.Add(id);

                        await session.StoreAsync(user, id);
                    }

                    Assert.Equal(3, shardNumToDocIds.Count);

                    await session.SaveChangesAsync();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                foreach (var dir in dirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                }

                // assert
                await AssertDocsInShardedDb(shardNumToDocIds, store2);

                // add more data to store1
                using (var session = store1.OpenAsyncSession())
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 100; i < 200; i++)
                    {
                        var user = new User { Name = i.ToString() };
                        var id = $"users/{i}";

                        var shardNumber = shardedCtx.GetShardNumber(context, id);
                        shardNumToDocIds[shardNumber].Add(id);

                        await session.StoreAsync(user, id);
                    }

                    await session.SaveChangesAsync();
                }

                waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import
                var newDirs = Directory.GetDirectories(backupPath).Except(dirs).ToList();
                Assert.Equal(3, newDirs.Count);

                foreach (var dir in newDirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                }

                // assert
                await AssertDocsInShardedDb(shardNumToDocIds, store2);
            }
        }

        private async Task AssertDocsInShardedDb(Dictionary<int, List<string>> shardNumToDocIds, IDocumentStore store)
        {
            foreach ((int shardNumber, List<string> ids) in shardNumToDocIds)
            {
                var shard = await GetDocumentDatabaseInstanceFor(store, $"{store.Database}${shardNumber}");
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsFrom(context, 0).ToList();
                    Assert.NotEmpty(docs);
                    Assert.Equal(ids.Count, docs.Count);

                    foreach (var doc in docs)
                    {
                        Assert.Contains(doc.Id, ids);
                    }
                }
            }
        }
    }
}
