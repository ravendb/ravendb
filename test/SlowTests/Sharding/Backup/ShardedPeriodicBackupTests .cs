using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Utils;
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

        private class Item
        {

        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupShardedAndExportToRegular()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                var backupPath = NewDataPath(suffix: "_BackupFolder");

                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" }))
                using (var store2 = Sharding.GetDocumentStore())
                using (var store3 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_3" }))
                {
                    await InsertData(store1, names);

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

                    //await CheckData(store2, names);

                    WaitHandle[] waitHandles = await WaitForBackupToComplete(store2);

                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                    await UpdateConfigurationAndRunBackupAsync(Server, store2, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // import

                    var dirs = Directory.GetDirectories(backupPath);
                    foreach (var dir in dirs)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    //await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupPath);

                    WaitForUserToContinueTheTest(store3);

                    await CheckData(store3, names);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupShardedAndExportToSharded()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                var backupPath = NewDataPath(suffix: "_BackupFolder");

                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" }))
                using (var store2 = Sharding.GetDocumentStore())
                using (var store3 = Sharding.GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_3" }))
                {
                    await InsertData(store1, names);

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

                    //await CheckData(store2, names);

                    WaitHandle[] waitHandles = await WaitForBackupToComplete(store2);

                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                    await UpdateConfigurationAndRunBackupAsync(Server, store2, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // import

                    var dirs = Directory.GetDirectories(backupPath);
                    foreach (var dir in dirs)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    //await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupPath);

                    WaitForUserToContinueTheTest(store3);

                    await CheckData(store3, names);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task<WaitHandle[]> WaitForBackupToComplete(IDocumentStore store)
        {
            var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
            var waitHandles = new WaitHandle[3];
            for (var i = 0; i < dbs.Count; i++)
            {
                var mre = new ManualResetEventSlim();
                waitHandles[i] = mre.WaitHandle;

                var db = await dbs[i];
                db.PeriodicBackupRunner._forTestingPurposes ??= new PeriodicBackupRunner.TestingStuff();
                db.PeriodicBackupRunner._forTestingPurposes.AfterBackupBatchCompleted = () => mre.Set();
            }

            return waitHandles;
        }

        private static async Task InsertData(IDocumentStore store, IReadOnlyList<string> names)
        {
            using (var session = store.OpenAsyncSession())
            {
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                });

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                //Time series
                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                //counters
                session.CountersFor("users/2").Increment("Downloads", 100);
                //Attachments
                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }

            //tombstone + revision
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("users/4");
                var user = await session.LoadAsync<User>("users/1");
                user.Age = 10;
                await session.SaveChangesAsync();
            }

            //subscription TODO aviv
            //await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            //Identity
            var result1 = store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

            //CompareExchange
            var user1 = new User
            {
                Name = "Toli"
            };
            var user2 = new User
            {
                Name = "Mitzi"
            };

            var operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/toli", user1, 0));
            operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/mitzi", user2, 0));
            var result = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("cat/mitzi", operationResult.Index));

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Efrat, DevelopmentHelper.Severity.Normal, "need to wait for sharding cluster transaction issue - RavenDB-13111");
            //Cluster transaction
            // using var session2 = store1.OpenAsyncSession(new SessionOptions
            // {
            //     TransactionMode = TransactionMode.ClusterWide
            // });
            //
            // var user4 = new User { Name = "Ayende" };
            // await session2.StoreAsync(user4);
            // await session2.StoreAsync(new { ReservedFor = user4.Id }, "usernames/" + user4.Name);
            //
            // await session2.SaveChangesAsync();

            //Index
            await new Index().ExecuteAsync(store);
        }

        private async Task CheckData(IDocumentStore store, IReadOnlyList<string> names)
        {
            var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
            //doc
            Assert.Equal(3, db.DocumentsStorage.GetNumberOfDocuments());
            //Assert.Equal(1, detailedStats.CountOfCompareExchangeTombstones); //TODO - Not working for 4.2
            //tombstone
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(1, db.DocumentsStorage.GetNumberOfTombstones(context));
                Assert.Equal(18, db.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context));
            }

            //Index
            var indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
            Assert.Equal(1, indexes.Length);

            //Subscriptions
            var subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10);
            //Assert.Equal(1, subscriptionDocuments.Count); //todo aviv - not working for backups

            using (var session = store.OpenSession())
            {
                //Time series
                var val = session.TimeSeriesFor("users/1", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/3", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                //Counters
                var counterValue = session.CountersFor("users/2").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < names.Count; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i + 1));
                    var metadata = session.Advanced.GetMetadataFor(user);

                    //Attachment
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (i == 0)
                    {
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 1)
                    {
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 2)
                    {
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }

                await session.StoreAsync(new User() { Name = "Toli" }, "users|");
                await session.SaveChangesAsync();
            }
            //Identity
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1991");
                Assert.NotNull(user);


            }
            //CompareExchange
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("cat/toli"));
                Assert.NotNull(user1);
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Efrat, DevelopmentHelper.Severity.Normal, "need to wait for sharding cluster transaction issue - RavenDB-13111");

                //TODO - need to wait for sharding cluster transaction issue - RavenDB-13111
                // user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/usernames/ayende"));
                // Assert.NotNull(user1);
                //
                // user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/users/1-a"));
                // Assert.NotNull(user1);
            }

        }


        private static async Task UpdateConfigurationAndRunBackupAsync(RavenServer server, IDocumentStore store, PeriodicBackupConfiguration config, bool isFullBackup = false)
        {
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

            var shards = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database);
            foreach (var shard in shards)
            {
                var documentDatabase = await shard;
                var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                periodicBackupRunner.StartBackupTask(result.TaskId, isFullBackup);
            }
        }
    }
}
