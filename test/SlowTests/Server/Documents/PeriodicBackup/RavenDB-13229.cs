using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_13229 : RavenTestBase
    {
        public RavenDB_13229(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupWithIdentityAndCompareExchangeShouldHaveOnlyOwnValues()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder1");
            var cmpXchg1 = new User { Name = "👺" };

            using (var store = GetDocumentStore(new Options { ModifyDatabaseName = s => "a" }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => "aa"
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor1"
                    };
                    await session.StoreAsync(bestUser, "a|");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/goblin", cmpXchg1, 0));

                using (var session = store2.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor2"
                    };
                    await session.StoreAsync(bestUser, "aa|");
                    await session.SaveChangesAsync();
                }

                var cmpXchg2 = new User { Name = "🤡" };
                await store2.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", cmpXchg2, 0));

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store); // FULL BACKUP
            }

            var backupDirectory = Directory.GetDirectories(backupPath).First();
            var databaseName = GetDatabaseName() + "restore";

            var files = Directory.GetFiles(backupDirectory)
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups()
                .ToArray();

            var restoreConfig = new RestoreBackupConfiguration
            {
                BackupLocation = backupDirectory,
                DatabaseName = databaseName,
                LastFileNameToRestore = files.Last()
            };

            using (var store2 = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = s => databaseName
            }))
            {
                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                store2.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion(TimeSpan.FromSeconds(30));
                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(1, stats.CountOfIdentities);
                    Assert.Equal(1, stats.CountOfCompareExchange);
                    Assert.Equal(1, stats.CountOfDocuments);

                    var bestUser = await session.LoadAsync<User>("a/1");
                    var mediocreUser1 = await session.LoadAsync<User>("aa/1");

                    Assert.NotNull(bestUser);
                    Assert.Null(mediocreUser1);

                    Assert.Equal("Egor1", bestUser.Name);

                    var cmpXchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/goblin");
                    Assert.Equal(cmpXchg1.Name, cmpXchg.Value.Name);
                    var cmpXchg2 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/clown");
                    Assert.Null(cmpXchg2);
                }
            }
        }

        [Fact]
        public void AllCompareExchangeAndIdentitiesPreserveAfterSchemaUpgradeFrom12()
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/Identities_CompareExchange_RavenData_from12.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(new ServerCreationOptions { DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false}))
            {
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var dbs = server.ServerStore.Cluster.GetDatabaseNames(context);
                    var dbsList = dbs.ToList();

                    Assert.Equal(2, dbsList.Count);
                    var dbName2 = dbsList[0];
                    Assert.Equal("a", dbName2);
                    var dbName1 = dbsList[1];
                    Assert.Equal("aa", dbName1);

                    var numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName1);
                    Assert.Equal(1, numOfIdentities);
                    numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName2);
                    Assert.Equal(1, numOfIdentities);

                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName1);
                    Assert.Equal(3, numOfCompareExchanges);
                    numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName2);
                    Assert.Equal(3, numOfCompareExchanges);
                }
            }
        }

        [Theory]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/Identities_CompareExchange_RavenData_from13.zip", 1024)]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/after_from12.zip", 1024)]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/after_from13.zip", 1024)]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/after_from14.zip", 1024)]
        [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenData_rc1_plus_additions.zip", 1026)]
        public void AllCompareExchangeAndIdentitiesPreserveAfterPreviousSchemaUpgrades(string filePath, int expectedCompareExchange)
        {
            var folder = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            DoNotReuseServer();

            var zipPath = new PathSetting(filePath);
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(new ServerCreationOptions {DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false}))
            {
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var dbs = server.ServerStore.Cluster.GetDatabaseNames(context);
                    var dbsList = dbs.ToList();

                    Assert.Equal(2, dbsList.Count);
                    var dbName1 = dbsList[0];
                    Assert.Equal("db1", dbName1);
                    var dbName2 = dbsList[1];
                    Assert.Equal("db2", dbName2);

                    var numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName1);
                    Assert.Equal(928, numOfIdentities);
                    numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName2);
                    Assert.Equal(948, numOfIdentities);

                    numOfIdentities = server.ServerStore.Cluster.GetIdentitiesFromPrefix(context, dbName1, 0, int.MaxValue).Count();
                    Assert.Equal(928, numOfIdentities);
                    numOfIdentities = server.ServerStore.Cluster.GetIdentitiesFromPrefix(context, dbName2, 0, int.MaxValue).Count();
                    Assert.Equal(948, numOfIdentities);

                    var numberOfCompareExchange = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName1);
                    Assert.Equal(expectedCompareExchange, numberOfCompareExchange);
                    numberOfCompareExchange = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName2);
                    Assert.Equal(expectedCompareExchange, numberOfCompareExchange);

                    numberOfCompareExchange = server.ServerStore.Cluster.GetCompareExchangeFromPrefix(context, dbName1, 0, int.MaxValue).Count();
                    Assert.Equal(expectedCompareExchange, numberOfCompareExchange);
                    numberOfCompareExchange = server.ServerStore.Cluster.GetCompareExchangeFromPrefix(context, dbName2, 0, int.MaxValue).Count();
                    Assert.Equal(expectedCompareExchange, numberOfCompareExchange);
                }
            }
        }
    }
}
