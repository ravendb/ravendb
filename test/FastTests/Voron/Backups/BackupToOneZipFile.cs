using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Voron.Impl.Backup;
using Voron.Util.Settings;

namespace FastTests.Voron.Backups
{
    public class BackupToOneZipFile : RavenLowLevelTestBase
    {
        [Fact(Skip = "Should add database record to backup and restore")]
        public async Task FullBackupToOneZipFile()
        {
            var tempFileName = NewDataPath(forceCreateDir: true);

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                var context = DocumentsOperationContext.ShortTermSingleUse(database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, database.Name, modifyConfiguration: configuration =>
                {
                    configuration.Collections["Users"].PurgeOnDelete = false;
                    configuration.Collections["Users"].MinimumRevisionsToKeep = 13;
                });

                await database.SubscriptionStorage.PutSubscription(new SubscriptionCreationOptions
                {
                    Query = "from Users",
                });

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

                var voronTempFileName = new VoronPathSetting(tempFileName);

                database.FullBackupTo(voronTempFileName.Combine("backup-test.backup").FullPath);
                BackupMethods.Full.Restore(voronTempFileName.Combine("backup-test.backup"), voronTempFileName.Combine("backup-test.data"));
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
    }
}
