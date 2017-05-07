using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Voron.Impl.Backup;
using Raven.Server.Json;

namespace FastTests.Voron.Backups
{
    public class BackupToOneZipFile : RavenLowLevelTestBase
    {
        [Fact(Skip="Should add database record to backup and restore")]
        public async Task FullBackupToOneZipFile()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempFileName);
            
            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                await VersioningHelper.SetupVersioning(Server.ServerStore, database.Name, false, 13);
                var subscriptionCriteria = new SubscriptionCriteria("Users");
                var obj = JObject.FromObject(subscriptionCriteria);
                var objString = obj.ToString(Formatting.None);
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(objString);
                streamWriter.Flush();
                stream.Position = 0;
                var reader = context.Read(stream, "docs/1");
                
                await database.SubscriptionStorage.CreateSubscription(JsonDeserializationServer.SubscriptionCreationParams(reader));

                await database.IndexStore.CreateIndex(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                });
                await database.IndexStore.CreateIndex(new IndexDefinition()
                {
                    Name = "Users_ByName2",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                });

                using (var tx = context.OpenWriteTransaction())
                {
                    var doc2 = CreateDocument(context, "users/2", new DynamicJsonValue
                    {
                        ["Name"] = "Edward",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Users"
                        }
                    });

                    database.DocumentsStorage.Put(context, "users/2", null, doc2);

                    tx.Commit();
                }
                
                foreach (var index in database.IndexStore.GetIndexes())
                {
                    index._indexStorage.Environment().Options.ManualFlushing = true;
                    index._indexStorage.Environment().FlushLogToDataFile();
                }
                database.DocumentsStorage.Environment.Options.ManualFlushing = true;
                database.DocumentsStorage.Environment.FlushLogToDataFile();

                database.FullBackupTo(Path.Combine(tempFileName, "backup-test.backup"));
                BackupMethods.Full.Restore(Path.Combine(tempFileName, "backup-test.backup"), Path.Combine(tempFileName, "backup-test.data"));
            }
            using (CreatePersistentDocumentDatabase(Path.Combine(tempFileName, "backup-test.data"), out var database))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                using (var tx = context.OpenReadTransaction())
                {
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/2"));
                    Assert.Equal(database.SubscriptionStorage.GetAllSubscriptionsCount(), 1);

                    var indexes = database.IndexStore.GetIndexes().ToList();
                    Assert.Equal(2, indexes.Count);
                    Assert.True(indexes.Any(x => x.Name == "Users_ByName"));
                    Assert.True(indexes.Any(x => x.Name == "Users_ByName2"));
                }
            }
        }

        [Fact(Skip = "Should add database record to backup and restore")]
        public async Task IncrementalBackupToOneZipFile()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempFileName);

            using (var database = CreateDocumentDatabase())
            {
                database.DocumentsStorage.Environment.Options.IncrementalBackupEnabled = true;
                database.DocumentsStorage.Environment.Options.ManualFlushing = true;
                
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {

                    var subscriptionCriteria = new SubscriptionCriteria("Users");
                    var obj = JObject.FromObject(subscriptionCriteria);
                    var objString = obj.ToString(Formatting.None);
                    var stream = new MemoryStream();
                    var streamWriter = new StreamWriter(stream);
                    streamWriter.Write(objString);
                    streamWriter.Flush();
                    stream.Position = 0;
                    var reader = context.Read(stream, "docs/1");
                    await database.SubscriptionStorage.CreateSubscription(JsonDeserializationServer.SubscriptionCreationParams(reader));

                    await database.IndexStore.CreateIndex(new IndexDefinition()
                    {
                        Name = "Users_ByName",
                        Maps = { "from user in docs.Users select new { user.Name }" },
                        Type = IndexType.Map
                    });

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().Options.ManualFlushing = true;
                        index._indexStorage.Environment().Options.IncrementalBackupEnabled = true;
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        var doc2 = CreateDocument(context, "users/2", new DynamicJsonValue
                        {
                            ["Name"] = "Edward",
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
                            }
                        });

                        database.DocumentsStorage.Put(context, "users/2", null, doc2);

                        tx.Commit();
                    }               

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().FlushLogToDataFile();
                    }
                    database.DocumentsStorage.Environment.FlushLogToDataFile();

                    database.IncrementalBackupTo(Path.Combine(tempFileName,
                        string.Format("voron-test.{0}-incremental-backup.zip", 0)));


                    await database.IndexStore.CreateIndex(new IndexDefinition()
                    {
                        Name = "Users_ByName2",
                        Maps = { "from user in docs.Users select new { user.Name }" },
                        Type = IndexType.Map
                    });

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().Options.ManualFlushing = true;
                        index._indexStorage.Environment().Options.IncrementalBackupEnabled = true;
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                        {
                            ["Name"] = "Edward",
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
                            }
                        });

                        database.DocumentsStorage.Put(context, "users/1", null, doc);

                        tx.Commit();
                    }                                        

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().FlushLogToDataFile();
                    }

                    database.DocumentsStorage.Environment.FlushLogToDataFile();

                    database.IncrementalBackupTo(Path.Combine(tempFileName,
                        string.Format("voron-test.{0}-incremental-backup.zip", 1)));

                    var forceUsing32BitsPager = database.Configuration.Storage.ForceUsing32BitsPager;
                    BackupMethods.Incremental.Restore(Path.Combine(tempFileName, "backup-test.data"), new[]
                    {
                        Path.Combine(tempFileName, "voron-test.0-incremental-backup.zip"),
                        Path.Combine(tempFileName, "voron-test.1-incremental-backup.zip")
                    }, options => options.ForceUsing32BitsPager = forceUsing32BitsPager);
                }
            }

            using (CreatePersistentDocumentDatabase(Path.Combine(tempFileName, "backup-test.data"), out var database))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                using (var tx = context.OpenReadTransaction())
                {
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/2"));
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/1"));
                    Assert.Equal(database.SubscriptionStorage.GetAllSubscriptionsCount(), 1);

                    var indexes = database.IndexStore.GetIndexes().ToList();
                    Assert.Equal(2, indexes.Count);
                    Assert.True(indexes.Any(x => x.Name == "Users_ByName"));
                    Assert.True(indexes.Any(x => x.Name == "Users_ByName2"));
                }
            }
        }
    }
}