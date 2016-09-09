using System;
using System.IO;
using System.IO.Compression;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Voron;
using Xunit;
using Voron.Impl.Backup;

namespace FastTests.Voron.Backups
{
    public class BackupToOneZipFile : RavenLowLevelTestBase
    {
        [Fact]
        public void FullBackupToOneZipFile()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempFileName);

            using (var database = CreateDocumentDatabase())
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);

                using (var tx = context.OpenWriteTransaction())
                {
                    var subscriptionCriteria = new SubscriptionCriteria
                    {
                        Collection = "Users",
                    };
                    var obj = RavenJObject.FromObject(subscriptionCriteria);
                    var objString = obj.ToString(Formatting.None);
                    var stream = new MemoryStream();
                    var streamWriter = new StreamWriter(stream);
                    streamWriter.Write(objString);
                    streamWriter.Flush();
                    stream.Position = 0;
                    var reader = context.Read(stream, "docs/1");
                    database.SubscriptionStorage.CreateSubscription(reader);

                    database.IndexStore.CreateIndex(new IndexDefinition()
                    {
                        Name = "Users_ByName",
                        Maps = { "from user in docs.Users select new { user.Name }" },
                        Type = IndexType.Map
                    });
                    database.IndexStore.CreateIndex(new IndexDefinition()
                    {
                        Name = "Users_ByName2",
                        Maps = { "from user in docs.Users select new { user.Name }" },
                        Type = IndexType.Map
                    });

                    var doc2 = CreateDocument(context, "users/2", new DynamicJsonValue
                    {
                        ["Name"] = "Edward",
                        [Constants.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Headers.RavenEntityName] = "Users"
                        }
                    });

                    database.DocumentsStorage.Put(context, "users/2", null, doc2);

                    tx.Commit();
                }

                database.SubscriptionStorage.Environment().Options.ManualFlushing = true;
                database.SubscriptionStorage.Environment().FlushLogToDataFile();

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
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: Path.Combine(tempFileName, "backup-test.data")))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                using (var tx = context.OpenReadTransaction())
                {
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/2"));
                    Assert.Equal(database.IndexStore.GetIndex(1).Name, "Users_ByName");
                    Assert.Equal(database.IndexStore.GetIndex(2).Name, "Users_ByName2");
                    Assert.Equal(database.SubscriptionStorage.GetAllSubscriptionsCount(), 1);
                }
            }
        }

       [Fact]
        public void IncrementalBackupToOneZipFile()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempFileName);

            using (var database = CreateDocumentDatabase())
            {
                database.DocumentsStorage.Environment.Options.IncrementalBackupEnabled = true;
                database.DocumentsStorage.Environment.Options.ManualFlushing = true;
                database.SubscriptionStorage.Environment().Options.ManualFlushing = true;
                database.SubscriptionStorage.Environment().Options.IncrementalBackupEnabled = true;
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        var subscriptionCriteria = new SubscriptionCriteria
                        {
                            Collection = "Users",
                        };
                        var obj = RavenJObject.FromObject(subscriptionCriteria);
                        var objString = obj.ToString(Formatting.None);
                        var stream = new MemoryStream();
                        var streamWriter = new StreamWriter(stream);
                        streamWriter.Write(objString);
                        streamWriter.Flush();
                        stream.Position = 0;
                        var reader = context.Read(stream, "docs/1");
                        database.SubscriptionStorage.CreateSubscription(reader);

                        database.IndexStore.CreateIndex(new IndexDefinition()
                        {
                            Name = "Users_ByName",
                            Maps = {"from user in docs.Users select new { user.Name }"},
                            Type = IndexType.Map
                        });

                        foreach (var index in database.IndexStore.GetIndexes())
                        {
                            index._indexStorage.Environment().Options.ManualFlushing = true;
                            index._indexStorage.Environment().Options.IncrementalBackupEnabled = true;
                        }

                        var doc2 = CreateDocument(context, "users/2", new DynamicJsonValue
                        {
                            ["Name"] = "Edward",
                            [Constants.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        });

                        database.DocumentsStorage.Put(context, "users/2", null, doc2);

                        tx.Commit();
                    }

                    database.SubscriptionStorage.Environment().FlushLogToDataFile();

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().FlushLogToDataFile();
                    }
                    database.DocumentsStorage.Environment.FlushLogToDataFile();

                    database.IncrementalBackupTo(Path.Combine(tempFileName,
                        string.Format("voron-test.{0}-incremental-backup.zip", 0)));

                    using (var tx = context.OpenWriteTransaction())
                    {
                        database.IndexStore.CreateIndex(new IndexDefinition()
                        {
                            Name = "Users_ByName2",
                            Maps = {"from user in docs.Users select new { user.Name }"},
                            Type = IndexType.Map
                        });

                        foreach (var index in database.IndexStore.GetIndexes())
                        {
                            index._indexStorage.Environment().Options.ManualFlushing = true;
                            index._indexStorage.Environment().Options.IncrementalBackupEnabled = true;
                        }

                        var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                        {
                            ["Name"] = "Edward",
                            [Constants.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Headers.RavenEntityName] = "Users"
                            }
                        });

                        database.DocumentsStorage.Put(context, "users/1", null, doc);

                        tx.Commit();
                    }

                    database.SubscriptionStorage.Environment().FlushLogToDataFile();

                    foreach (var index in database.IndexStore.GetIndexes())
                    {
                        index._indexStorage.Environment().FlushLogToDataFile();
                    }

                    database.DocumentsStorage.Environment.FlushLogToDataFile();

                    database.IncrementalBackupTo(Path.Combine(tempFileName,
                        string.Format("voron-test.{0}-incremental-backup.zip", 1)));

                    BackupMethods.Incremental.Restore(Path.Combine(tempFileName, "backup-test.data"), new[]
                    {
                        Path.Combine(tempFileName, "voron-test.0-incremental-backup.zip"),
                        Path.Combine(tempFileName, "voron-test.1-incremental-backup.zip")
                    });
                }
            }
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: Path.Combine(tempFileName, "backup-test.data")))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                using (var tx = context.OpenReadTransaction())
                {
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/2"));
                    Assert.NotNull(database.DocumentsStorage.Get(context, "users/1"));
                    Assert.Equal(database.IndexStore.GetIndex(1).Name, "Users_ByName");
                    Assert.Equal(database.IndexStore.GetIndex(2).Name, "Users_ByName2");
                    Assert.Equal(database.SubscriptionStorage.GetAllSubscriptionsCount(), 1);
                }
            }
        }
    }
}