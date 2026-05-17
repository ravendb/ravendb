using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26585 : ClusterTestBase
    {
        public RavenDB_26585(ITestOutputHelper output) : base(output)
        {
        }

        // ========================================================================
        // Fix AssertNumberOfSubscriptionsPerClusterLimits had an inverted
        // condition (`> max == false`) that caused a LicenseLimitException even
        // when the cluster's subscription count was well below the limit.
        // ========================================================================

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport | RavenTestCategory.Subscriptions)]
        public async Task RestoreWithSubscriptionsOnCommunityLicenseShouldNotFail()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = "Sub1",
                        Query = "from Users"
                    });

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    // Before the fix, restoring even a single subscription threw:
                    //   "The maximum number of subscriptions per cluster cannot exceed the limit of: 15"
                    // because of the inverted `>` check. After the fix this should succeed.
                    RunRestore(store, backupPath);
                }
            }
            finally
            {
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport | RavenTestCategory.Subscriptions)]
        public async Task ImportingSubscriptionsOnCommunityLicenseShouldNotFail()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);

                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = "ImportSub",
                        Query = "from Users"
                    });

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var subs = await store.Subscriptions.GetSubscriptionsAsync(0, 10);
                    Assert.Contains(subs, s => s.SubscriptionName == "ImportSub");
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Subscriptions)]
        public async Task SinglePutSubscriptionEnforcesPerDatabaseLimitOnCommunityLicense()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var maxPerDatabase = Server.ServerStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerDatabase;
                Assert.NotNull(maxPerDatabase);
                Assert.True(maxPerDatabase.Value > 0,
                    $"Expected community license to define a positive per-database subscription limit, got {maxPerDatabase}.");

                // Create exactly the maximum allowed - this should succeed via the single-Put path.
                for (var i = 0; i < maxPerDatabase.Value; i++)
                {
                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = $"Sub_{i}",
                        Query = "from Users"
                    });
                }

                // The next single put should trigger the per-database limit.
                // Before the fix, count checks were skipped on the single-Put path
                // and this would (incorrectly) succeed.
                var ex = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = "Sub_OverLimit",
                        Query = "from Users"
                    });
                });

                Assert.Equal(LimitType.Subscriptions, ex.LimitType);
                Assert.Contains("per database", ex.Message);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes)]
        public async Task StaticIndexCreationOnCommunityLicenseStillWorks()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Name = "x" });
                    session.SaveChanges();
                }

                await new Items_ByName().ExecuteAsync(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var found = session.Query<Item, Items_ByName>().Count();
                    Assert.Equal(1, found);
                }
            }
        }


        // ========================================================================
        // Fix  AssertQueueSink used to throw whenever any QueueSink existed in
        // the database record, even if every entry was disabled. 
        // ========================================================================

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ImportingDisabledQueueSinkOnCommunityLicenseShouldNotFail()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await SetupDisabledQueueSink(store);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    // Before the fix this throws LicenseLimitException(QueueSink) even though
                    // the imported queue sink is disabled.
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task RestoreWithDisabledQueueSinkOnCommunityLicenseShouldNotFail()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            try
            {
                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                    await SetupDisabledQueueSink(store);

                    await RunBackup(store, backupPath);
                }

                using (var store = GetDocumentStore())
                {
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                    RunRestore(store, backupPath);
                }
            }
            finally
            {
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task LicenseDowngradeWithDefaultRevisionsConfigurationIsRejected()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.DisableRevisionCompression(Server, store);

                // Configure a Default with a high MinimumRevisionsToKeep and a large age.
                // Exact values are picked to clearly exceed any per-license cap.
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10_000,
                        MinimumRevisionAgeToKeep = TimeSpan.FromDays(10_000)
                    }
                };

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                // Switching to community must be rejected with a RevisionsConfiguration limit type.
                // (Either because community doesn't allow Default at all, or — if it does — because
                // the Default's values exceed the license cap. Either path now ends in the same
                // limit type after the Default-validation fix.)
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.RevisionsConfiguration);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Subscriptions)]
        public async Task LicenseDowngradeOverPerDatabaseSubscriptionCapIsRejected()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.DisableRevisionCompression(Server, store);

                for (var i = 0; i < 10; i++)
                {
                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = $"DowngradeSub_{i}",
                        Query = "from Users"
                    });
                }

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Subscriptions);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Subscriptions)]
        public async Task LicenseDowngradeOverPerClusterSubscriptionCapIsRejected()
        {
            DoNotReuseServer();

            // Spread subscriptions across multiple databases so we trip the per-cluster
            // cap (community: 15) without each database individually busting the per-DB cap.
            var stores = new List<DocumentStore>();
            try
            {
                // 10 databases x 2 subs = 20 total subs across the cluster, comfortably over
                // the community per-cluster cap of 15.
                for (var dbIndex = 0; dbIndex < 10; dbIndex++)
                {
                    var store = GetDocumentStore();
                    stores.Add(store);

                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = "Sub_a",
                        Query = "from Users"
                    });
                    await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                    {
                        Name = "Sub_b",
                        Query = "from Users"
                    });
                    await LicenseHelper.DisableRevisionCompression(Server, store);
                }

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Subscriptions);
            }
            finally
            {
                foreach (var s in stores)
                    s.Dispose();
            }
        }

        // ------------------------------------------------------------------------
        // Helpers
        // ------------------------------------------------------------------------

        private async Task SetupDisabledQueueSink(DocumentStore store)
        {
            var connectionString = new QueueConnectionString
            {
                Name = "QueueSinkConn",
                BrokerType = QueueBrokerType.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings { BootstrapServers = "localhost:9092" }
            };
            var putCs = await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(connectionString));
            Assert.NotNull(putCs.RaftCommandIndex);

            var configuration = new QueueSinkConfiguration
            {
                Disabled = true,
                Name = "DisabledQueueSink",
                ConnectionStringName = "QueueSinkConn",
                BrokerType = QueueBrokerType.Kafka,
                Scripts =
                {
                    new QueueSinkScript
                    {
                        Name = "Script",
                        Queues = new List<string> { "my-queue" },
                        Script = "this.Foo = 1;"
                    }
                }
            };

            var addResult = await store.Maintenance.SendAsync(new AddQueueSinkOperation<QueueConnectionString>(configuration));
            Assert.NotEqual(0, addResult.TaskId);
        }

        private void RunRestore(DocumentStore store, string backupPath)
        {
            var configuration = new RestoreBackupConfiguration
            {
                DatabaseName = store.Database + "_restored",
                BackupLocation = Directory.GetDirectories(backupPath).First()
            };

            Backup.RestoreDatabase(store, configuration);
        }

        private static async Task RunBackup(DocumentStore store, string backupPath)
        {
            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
        }

        private class Item
        {
            public string Name { get; set; }
        }

        private class Items_ByName : AbstractIndexCreationTask<Item>
        {
            public Items_ByName()
            {
                Map = items => from i in items select new { i.Name };
            }
        }
    }
}
