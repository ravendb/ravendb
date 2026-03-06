using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.Sorters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.QueueSink;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Transformation = Raven.Client.Documents.Operations.ETL.Transformation;

namespace SlowTests.Issues
{
    public class RavenDB_21427 : ReplicationTestBase
    {
        public RavenDB_21427(ITestOutputHelper output) : base(output)
        {
        }

        // ----------------------------------------
        // Tests for Sharding License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Sharding)]
        public async Task Prevent_License_Downgrade_Multi_Node_Sharding()
        {
            DoNotReuseServer();

            var (_, leader) = await CreateRaftCluster(2, false, watcherCluster: true);
            var options = Options.ForMode(RavenDatabaseMode.Sharded);
            options.Server = leader;
            options.ReplicationFactor = 2;
            options.ModifyDatabaseRecord = r =>
            {
                r.Sharding ??= new ShardingConfiguration();
                r.Sharding.Shards = Enumerable.Range(0, 5)
                    .Select((shardNumber) => new KeyValuePair<int, DatabaseTopology>(shardNumber, new DatabaseTopology())).ToDictionary(x => x.Key, x => x.Value);
            };

            using (GetDocumentStore(options))
            {
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_COMM, LimitType.Sharding);
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_DEV, LimitType.Sharding);
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_PRO, LimitType.Sharding);
            }
        }

        // ----------------------------------------
        // Tests for Data Archival License Limits
        // ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_DataArchival()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

                await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.DataArchival);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.DataArchival);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        // ----------------------------------------
        // Tests for Indexes License Limits
        // ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_License_Downgrade_Static_Index_Count_Per_Database()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 15; i++)
                {
                    store.Maintenance.Send(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + i }
                    }));
                }
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Indexes);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_Put_Static_Index_Max_Per_Database()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                for (int i = 0; i < 12; i++)
                {
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + i }));
                }
                Assert.Throws<IndexCreationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test12" })));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_License_Downgrade_Static_Index_Count_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            try
            {
                for (int i = 0; i < 7; i++)
                {
                    var store = GetDocumentStore();
                    storeList.Add(store);
                    for (int j = 0; j < 10; j++)
                    {
                        store.Maintenance.Send(new PutIndexesOperation(new[]
                        {
                            new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + j }
                        }));
                    }
                }

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Indexes);
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_Put_Static_Index_Max_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            try
            {
                for (int i = 0; i < 6; i++)
                {
                    DocumentStore store = GetDocumentStore();
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    storeList.Add(store);
                    for (int j = 0; j < 10; j++)
                    {
                        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test" + j }));
                    }
                }
                Assert.Throws<IndexCreationException>(() => storeList[0].Maintenance.Send(new PutIndexesOperation(new IndexDefinition { Maps = { "from doc in docs.Images select new { doc.Tags }" }, Name = "test44" })));
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_License_Downgrade_Auto_Index_Count_Per_Database()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                for (int j = 0; j < 28; j++)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + j } }),
                        Guid.NewGuid().ToString());
                }
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Indexes);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_Put_Auto_Index_Max_Per_Database()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                DocumentDatabase database;
                for (int j = 0; j < 24; j++)
                {
                    database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + j } }),
                        Guid.NewGuid().ToString());
                }
                database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await Assert.ThrowsAsync<IndexCreationException>(async () => await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" } }),
                    Guid.NewGuid().ToString()));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_License_Downgrade_Auto_Index_Count_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            try
            {
                for (int i = 0; i < 7; i++)
                {
                    var store = GetDocumentStore();
                    storeList.Add(store);
                    for (int j = 0; j < 20; j++)
                    {
                        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                        await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + j } }),
                            Guid.NewGuid().ToString());
                    }
                }

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Indexes);
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Skip = "No count restriction at the moment")]
        public async Task Prevent_Put_Auto_Index_Max_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            DocumentDatabase database;
            try
            {
                for (int i = 0; i < 6; i++)
                {
                    var store = GetDocumentStore();
                    await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                    storeList.Add(store);
                    for (int j = 0; j < 20; j++)
                    {
                        database = await Databases.GetDocumentDatabaseInstanceFor(store);
                        await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" + i + j } }),
                            Guid.NewGuid().ToString());
                    }
                }
                database = await Databases.GetDocumentDatabaseInstanceFor(storeList[0]);
                await Assert.ThrowsAsync<LicenseLimitException>(async () => await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name" } }),
                    Guid.NewGuid().ToString()));
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Architecture = RavenArchitecture.AllX64)]
        public async Task Prevent_License_Downgrade_Index_Additional_Assemblies()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test",
                    AdditionalAssemblies = { AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0") }
                }));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.AdditionalAssembliesFromNuGet);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Indexes, Architecture = RavenArchitecture.AllX64)]
        public async Task Prevent_Put_Index_Additional_Assemblies()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var exception = Assert.Throws<LicenseLimitException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test",
                    AdditionalAssemblies = { AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0") }
                })));
                Assert.Equal(LimitType.AdditionalAssembliesFromNuGet, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs.Images select new { doc.Tags }" },
                    Name = "test",
                    AdditionalAssemblies = { AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0") }
                }));
            }
        }

        // ----------------------------------------
        // Tests for Revision License Limits
        // ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_License_Downgrade_Revision_Default_Configuration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.RevisionsConfiguration);

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.RevisionsConfiguration);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_Put_Revision()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration)));
                Assert.Equal(LimitType.RevisionsConfiguration, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 0 } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_License_Downgrade_Revision_Compression()
        {
            DoNotReuseServer();
            using (GetDocumentStore())
            {
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_COMM));
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                 exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO));
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_Enable_Revision_Compression()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var configuration = new DocumentsCompressionConfiguration { CompressRevisions = true, Collections = new string[] { } };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await Server.ServerStore.SendToLeaderAsync(new EditDocumentsCompressionCommand(configuration, store.Database, RaftIdGenerator.DontCareId)));
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);

                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await Server.ServerStore.SendToLeaderAsync(new EditDocumentsCompressionCommand(configuration, store.Database, RaftIdGenerator.DontCareId)));
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await Server.ServerStore.SendToLeaderAsync(new EditDocumentsCompressionCommand(configuration, store.Database, RaftIdGenerator.DontCareId));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Put_Disabled_Revisions()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = true, MinimumRevisionsToKeep = 0 } };
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));
            }
        }

        // ----------------------------------------
        // Tests for Backup License Limits
        // ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_PeriodicBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.PeriodicBackup);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_Put_PeriodicBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config)));
                Assert.Equal(LimitType.PeriodicBackup, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_Encrypted_Backup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath,
                    fullBackupFrequency: "* */1 * * *",
                    incrementalBackupFrequency: "* */2 * * *",
                    backupEncryptionSettings: new BackupEncryptionSettings()
                    {
                        EncryptionMode = EncryptionMode.UseProvidedKey,
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    });
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.EncryptedBackup);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_Snapshot()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath,
                    fullBackupFrequency: "* */1 * * *",
                    incrementalBackupFrequency: "* */2 * * *",
                    backupType: BackupType.Snapshot);

                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.SnapshotBackup);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.SnapshotBackup);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_Put_Snapshot_Backup()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath("SnapshotBackup")
                    }
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config)));
                Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config)));
                Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_Snapshot_Backup()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath("SnapshotBackup")
                    }
                };
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.SnapshotBackup);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.SnapshotBackup);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_Put_Cloud_Backup()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    GoogleCloudSettings = new GoogleCloudSettings
                    {
                        BucketName = "dummy-bucket",
                        GoogleCredentialsJson = "{}", // placeholder
                        RemoteFolderName = "backups"
                    }
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config)));
                Assert.Equal(LimitType.CloudBackup, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Prevent_License_Downgrade_Cloud_Backup()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    GoogleCloudSettings = new GoogleCloudSettings
                    {
                        BucketName = "dummy-bucket",
                        GoogleCredentialsJson = "{}", // placeholder
                        RemoteFolderName = "backups"
                    }
                };
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.CloudBackup);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task Put_Disabled_PeriodicBackup()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", disabled: true);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
            }
        }

        // ----------------------------------------
        // Tests for Sorters License Limits
        // ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Sorters_Per_Database()
        {
            DoNotReuseServer();
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName,
                ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                       {
                           {
                               "MySorter",
                               new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                           },
                           {
                               "MySorter2",
                               new SorterDefinition { Name = sorterName + "2", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter2", sorterName + "2") }
                           }
                       }
            }))
            {
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.CustomSorters);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_Sorters_Per_Database()
        {
            DoNotReuseServer();
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var sorterCode1 = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName);
                var sorterCode2 = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "2");

                await store.Maintenance.SendAsync(new PutSortersOperation(new SorterDefinition { Name = sorterName, Code = sorterCode1 }));

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.SendAsync(new PutSortersOperation(new SorterDefinition { Name = sorterName + "2", Code = sorterCode2 })));
                Assert.Equal(LimitType.CustomSorters, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new PutSortersOperation(new SorterDefinition { Name = sorterName + "2", Code = sorterCode2 }));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Sorters_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var sorterName = GetDatabaseName();
            var storeList = new List<DocumentStore>();
            try
            {
                for (int i = 0; i < 6; i++)
                {
                    var store = GetDocumentStore(new Options
                    {
                        ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                        {
                            {
                                "MySorter", new SorterDefinition { Name = sorterName + "1", Code = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName + "1") }
                            }
                        }
                    });
                    storeList.Add(store);
                }
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.CustomSorters);
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        // ----------------------------------------
        // Tests for Analyzer License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Analyzer_Per_Database()
        {
            DoNotReuseServer();
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName,
                ModifyDatabaseRecord = record => record.Analyzers = record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                {
                    ["MyAnalyzer"] = new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer",
                        Code = @"
            using Lucene.Net.Analysis;
            using Lucene.Net.Analysis.Standard;
            using System.IO;

            public class MyAnalyzer : Analyzer
            {
                public override TokenStreamComponents CreateComponents(string fieldName, TextReader reader)
                {
                    return new TokenStreamComponents(new StandardTokenizer(Lucene.Net.Util.LuceneVersion.LUCENE_48, reader));
                }
            }"
                    },
                    ["MyAnalyzer2"] = new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer2",
                        Code = @"
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Core;
using System.IO;

public class MyAnalyzer2 : Analyzer
{
    public override TokenStreamComponents CreateComponents(string fieldName, TextReader reader)
    {
        return new TokenStreamComponents(new WhitespaceTokenizer(Lucene.Net.Util.LuceneVersion.LUCENE_48, reader));
    }
}"
                    }
                }
            }))
            {
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.CustomAnalyzers);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Analyzer_Per_Cluster()
        {
            DoNotReuseServer();
            await CreateRaftCluster(3, false, watcherCluster: true);
            var storeList = new List<DocumentStore>();
            try
            {
                for (int i = 0; i < 6; i++)
                {
                    var store = GetDocumentStore(new Options
                    {
                        ModifyDatabaseRecord = record => record.Analyzers = record.Analyzers = new Dictionary<string, AnalyzerDefinition>
                        {
                            ["MyAnalyzer"] = new AnalyzerDefinition
                            {
                                Name = "MyAnalyzer",
                                Code = @"
            using Lucene.Net.Analysis;
            using Lucene.Net.Analysis.Standard;
            using System.IO;

            public class MyAnalyzer : Analyzer
            {
                public override TokenStreamComponents CreateComponents(string fieldName, TextReader reader)
                {
                    return new TokenStreamComponents(new StandardTokenizer(Lucene.Net.Util.LuceneVersion.LUCENE_48, reader));
                }
            }"
                            }
                        }
                    });

                    storeList.Add(store);
                }
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.CustomAnalyzers);
            }
            finally
            {
                foreach (var store in storeList)
                {
                    store.Dispose();
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_Analyzer_Per_Database()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                await store.Maintenance.SendAsync(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = "MyAnalyzer2",
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", "MyAnalyzer2")
                }));
                var exception = Assert.Throws<LicenseLimitException>(() =>
                    store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition
                    {
                        Name = "MyAnalyzer",
                        Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", "MyAnalyzer")
                    })));

                Assert.Equal(LimitType.CustomAnalyzers, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new PutAnalyzersOperation(new AnalyzerDefinition
                {
                    Name = "MyAnalyzer",
                    Code = GetAnalyzer("RavenDB_14939.MyAnalyzer.cs", "MyAnalyzer", "MyAnalyzer")
                }));
            }
        }
        private static string GetAnalyzer(string resourceName, string originalAnalyzerName, string analyzerName)
        {
            using (var stream = GetDump(resourceName))
            using (var reader = new StreamReader(stream))
            {
                var analyzerCode = reader.ReadToEnd();
                analyzerCode = analyzerCode.Replace(originalAnalyzerName, analyzerName);

                return analyzerCode;
            }
        }

        // ----------------------------------------
        // Tests for Client Configuration License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ClientApi)]
        public async Task Prevent_License_Downgrade_ClientConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = r => r.Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50 } }))
            {
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.ClientConfiguration);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_ClientConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var config = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50 };

                var command = new PutDatabaseClientConfigurationCommand(config, store.Database, RaftIdGenerator.NewId());
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await Server.ServerStore.SendToLeaderAsync(command));
                Assert.Equal(LimitType.ClientConfiguration, exception.LimitType);

                command = new PutDatabaseClientConfigurationCommand(config, store.Database, RaftIdGenerator.NewId());
                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Put_Disabled_ClientConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var config = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50, Disabled = true };

                var command = new PutDatabaseClientConfigurationCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_ServerWideClientConfiguration()
        {
            DoNotReuseServer();
            var server = GetNewServer();
            var options = new Options() { Server = server };
            using (var store = GetDocumentStore(options))
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(server, store, LicenseTestBase.RL_COMM);

                var config = new ClientConfiguration { MaxNumberOfRequestsPerSession = 50 };

                var command = new PutClientConfigurationCommand(config, RaftIdGenerator.NewId());
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await server.ServerStore.SendToLeaderAsync(command));
                Assert.Equal(LimitType.ClientConfiguration, exception.LimitType);

                command = new PutClientConfigurationCommand(config, RaftIdGenerator.NewId());
                await LicenseHelper.PutLicense(server, LicenseTestBase.RL_PRO);
                await server.ServerStore.SendToLeaderAsync(command);
            }
        }

        // ----------------------------------------
        // Tests for Studio Configuration License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_StudioConfiguration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var command = new PutDatabaseStudioConfigurationCommand(new ServerWideStudioConfiguration() { DisableAutoIndexCreation = true, }, store.Database,
                    RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.StudioConfiguration);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_StudioConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var command = new PutDatabaseStudioConfigurationCommand(
                    new ServerWideStudioConfiguration { DisableAutoIndexCreation = true },
                    store.Database, RaftIdGenerator.NewId());

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await Server.ServerStore.SendToLeaderAsync(command));
                Assert.Equal(LimitType.StudioConfiguration, exception.LimitType);

                command = new PutDatabaseStudioConfigurationCommand(
                    new ServerWideStudioConfiguration { DisableAutoIndexCreation = true },
                    store.Database, RaftIdGenerator.NewId());
                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Put_Disabled_StudioConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var command = new PutDatabaseStudioConfigurationCommand(
                    new ServerWideStudioConfiguration { DisableAutoIndexCreation = true, Disabled = true },
                    store.Database, RaftIdGenerator.NewId());

                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        // ----------------------------------------
        // Tests for Expiration License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_Expiration_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var config = new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100, };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Expiration);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_ExpirationConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 60 };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config));
                Assert.Equal(LimitType.Expiration, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Put_Disabled_ExpirationConfiguration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new ExpirationConfiguration { Disabled = true, DeleteFrequencyInSec = 60 };
                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
            }
        }

        // ----------------------------------------
        // Tests for Refresh License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_License_Downgrade_Refresh_Configuration()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = false };
                await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Refresh);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Prevent_Put_Refresh_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = false };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig)));

                Assert.Equal(LimitType.Refresh, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.ExpirationRefresh)]
        public async Task Put_Disabled_Refresh_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var refConfig = new RefreshConfiguration { RefreshFrequencyInSec = 33, Disabled = true };
                await store.Maintenance.SendAsync(new ConfigureRefreshOperation(refConfig));
            }
        }
        
        // ----------------------------------------
        // Tests for Encryption License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Encryption)]
        public async Task Prevent_License_Downgrade_Encryption()
        {
            DoNotReuseServer();
            string dbName = Encryption.SetupEncryptedDatabase(out var certificates, out var _);
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = _ => dbName,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                Path = NewDataPath()
            }))
            {
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.Encryption);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.Encryption);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        // ----------------------------------------
        // Tests for Dynamic Nodes Distribution License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Sharding)]
        public async Task Prevent_Put_Dynamic_Node_Distribution()
        {
            DoNotReuseServer();
            var (nodes, leader) = await CreateRaftCluster(3);
            var options = Options.ForMode(RavenDatabaseMode.Sharded);
            options.Server = leader;
            options.ModifyDatabaseRecord = record =>
            {
                record.Topology = new DatabaseTopology
                {
                    DynamicNodesDistribution = true,
                    Members = new List<string> { "A", "B", "C" },
                    ReplicationFactor = 3
                };
            };

            using (var store = GetDocumentStore(options))
            {
                var res = await WaitForValueAsync(async () =>
                {
                    var sum = 0;
                    foreach (var node in nodes)
                    {
                        using var perNodeStore = new DocumentStore
                        {
                            Urls = new[] { node.WebUrl },
                            Database = store.Database,
                            Conventions = store.Conventions
                        }.Initialize();

                        var recored = await perNodeStore.Maintenance.Server.SendAsync(
                            new GetDatabaseRecordOperation(store.Database));

                        if (recored.Topology.DynamicNodesDistribution)
                            sum++;
                    }
                    return sum;
                }, 3, timeout: 50_000);

                Assert.Equal(3, res);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.PutLicense(leader, LicenseTestBase.RL_COMM);
                });

                Assert.Equal(LimitType.DynamicNodeDistribution, exception.LimitType);

                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    await LicenseHelper.PutLicense(leader, LicenseTestBase.RL_PRO);
                });

                Assert.Equal(LimitType.DynamicNodeDistribution, exception.LimitType);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(leader, store, LicenseTestBase.RL_DEV);
            }
        }

        // ----------------------------------------
        // Tests for Documents Compression License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Prevent_Put_Revision_Compression()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new DocumentsCompressionConfiguration
                {
                    CompressRevisions = true,
                    Collections = new[] { "Users" }
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await Server.ServerStore.SendToLeaderAsync(
                        new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()))
                );
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await Server.ServerStore.SendToLeaderAsync(
                        new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()))
                );
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_Put_Document_Compression()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var config = new DocumentsCompressionConfiguration
                {
                    CompressAllCollections = true,
                    Collections = new string[] { }
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await Server.ServerStore.SendToLeaderAsync(
                        new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()))
                );
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await Server.ServerStore.SendToLeaderAsync(
                        new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()))
                );
                Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Prevent_License_Downgrade_Document_Compression()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var config = new DocumentsCompressionConfiguration
                {
                    CompressAllCollections = true,
                    Collections = new string[]{}
                };
                await Server.ServerStore.SendToLeaderAsync(
                    new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.DocumentsCompression);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.DocumentsCompression);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing)]
        public async Task Put_Disabled_Document_Compression()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new DocumentsCompressionConfiguration
                {
                    CompressAllCollections = false,
                    Collections = new string[] { }
                };
                await Server.ServerStore.SendToLeaderAsync(
                    new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Revisions)]
        public async Task Put_Disabled_Revision_Compression()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var config = new DocumentsCompressionConfiguration
                {
                    CompressRevisions = false,
                    CompressAllCollections = false,
                    Collections = new string[] { }
                };
                await Server.ServerStore.SendToLeaderAsync(
                    new EditDocumentsCompressionCommand(config, store.Database, RaftIdGenerator.NewId()));
            }
        }

        // ----------------------------------------
        // Tests for ETL License Limits
        //  ---------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_Raven_ETL()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var connectionString = new RavenConnectionString
                {
                    Name = "RavenConnStr",
                    Database = store.Database,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                var config = new RavenEtlConfiguration
                {
                    Name = "RavenEtlTask",
                    ConnectionStringName = "RavenConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Users" },
                            Script = null
                        }
                    }
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(config)));
                Assert.Equal(LimitType.RavenEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_Raven_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var connectionString = new RavenConnectionString
                {
                    Name = "RavenConnStr",
                    Database = store.Database,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));

                var config = new RavenEtlConfiguration
                {
                    Name = "RavenEtlTask",
                    ConnectionStringName = "RavenConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Users" },
                            Script = null
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.RavenEtl);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_Sql_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new SqlConnectionString
                {
                    Name = "SqlConnStr",
                    FactoryName = "System.Data.SqlClient",
                    ConnectionString = "Server=localhost;Database=Test;User Id=sa;Password=123456;"
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

                var config = new SqlEtlConfiguration
                {
                    Name = "SqlEtlTask",
                    ConnectionStringName = "SqlConnStr",
                    SqlTables = { new SqlEtlTable { TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false } },
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Users" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(config)));
                Assert.Equal(LimitType.SqlEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_Sql_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.CreateSqlEtlConfiguration(store);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.SqlEtl);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
                        }
                    }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_Olap_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new OlapConnectionString
                {
                    Name = "OlapConnStr",
                    LocalSettings = new LocalSettings
                        {
                            FolderPath = NewDataPath(suffix: "OlapOutput")
                        }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<OlapConnectionString>(connectionString));

                var config = new OlapEtlConfiguration
                {
                    Name = "OlapEtlTask",
                    ConnectionStringName = "OlapConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "OlapTransform",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<OlapConnectionString>(config)));

                Assert.Equal(LimitType.OlapEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<OlapConnectionString>(config)));

                Assert.Equal(LimitType.OlapEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new AddEtlOperation<OlapConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_Olap_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var connectionString = new OlapConnectionString
                {
                    Name = "OlapConnStr",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath(suffix: "OlapOutput")
                    }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<OlapConnectionString>(connectionString));

                var config = new OlapEtlConfiguration
                {
                    Name = "OlapEtlTask",
                    ConnectionStringName = "OlapConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "OlapTransform",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<OlapConnectionString>(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.OlapEtl);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.OlapEtl);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_Elasticsearch_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new ElasticSearchConnectionString
                {
                    Name = "ElasticConnStr",
                    Nodes = new[] { "http://localhost:9200" }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<ElasticSearchConnectionString>(connectionString));

                var config = new ElasticSearchEtlConfiguration
                {
                    Name = "ElasticEtlTask",
                    ConnectionStringName = "ElasticConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Products" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<ElasticSearchConnectionString>(config)));

                Assert.Equal(LimitType.ElasticSearchEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<ElasticSearchConnectionString>(config)));

                Assert.Equal(LimitType.ElasticSearchEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new AddEtlOperation<ElasticSearchConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_Elasticsearch_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var connectionString = new ElasticSearchConnectionString
                {
                    Name = "ElasticConnStr",
                    Nodes = new[] { "http://localhost:9200" }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<ElasticSearchConnectionString>(connectionString));

                var config = new ElasticSearchEtlConfiguration
                {
                    Name = "ElasticEtlTask",
                    ConnectionStringName = "ElasticConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Products" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<ElasticSearchConnectionString>(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.ElasticSearchEtl);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.ElasticSearchEtl);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_Queue_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new QueueConnectionString
                {
                    Name = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    KafkaConnectionSettings = new KafkaConnectionSettings
                    {
                        BootstrapServers = "localhost:9092"
                    }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(connectionString));

                var config = new QueueEtlConfiguration
                {
                    Name = "QueueEtlTask",
                    ConnectionStringName = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "QueueScript",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<QueueConnectionString>(config)));

                Assert.Equal(LimitType.QueueEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new AddEtlOperation<QueueConnectionString>(config)));
                Assert.Equal(LimitType.QueueEtl, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new AddEtlOperation<QueueConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_Queue_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var connectionString = new QueueConnectionString
                {
                    Name = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    KafkaConnectionSettings = new KafkaConnectionSettings
                    {
                        BootstrapServers = "localhost:9092"
                    }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(connectionString));

                var config = new QueueEtlConfiguration
                {
                    Name = "QueueEtlTask",
                    ConnectionStringName = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "QueueScript",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<QueueConnectionString>(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.QueueEtl);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.QueueEtl);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_License_Downgrade_QueueSink()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(
                    new PutConnectionStringOperation<QueueConnectionString>(
                        new QueueConnectionString
                        {
                            Name = "KafkaConStr",
                            BrokerType = QueueBrokerType.Kafka,
                            KafkaConnectionSettings = new KafkaConnectionSettings() { BootstrapServers = "localhost:9092" }
                        }));

                QueueSinkScript queueSinkScript = new()
                {
                    Name = "orders",
                    Queues = new List<string>() { "orders" },
                    Script = @"this['@metadata']['@collection'] = 'Orders';
               put(this.Id.toString(), this)"
                };

                var config = new QueueSinkConfiguration() { ConnectionStringName = "KafkaConStr", BrokerType = QueueBrokerType.Kafka, Scripts = { queueSinkScript } };

                store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.QueueSink);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.QueueSink);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Prevent_Put_QueueSink()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                store.Maintenance.Send(
                    new PutConnectionStringOperation<QueueConnectionString>(
                        new QueueConnectionString
                        {
                            Name = "KafkaConStr",
                            BrokerType = QueueBrokerType.Kafka,
                            KafkaConnectionSettings = new KafkaConnectionSettings() { BootstrapServers = "localhost:9092" }
                        }));

                QueueSinkScript queueSinkScript = new QueueSinkScript
                {
                    Name = "orders",
                    Queues = new List<string>() { "orders" },
                    Script = @"this['@metadata']['@collection'] = 'Orders';
               put(this.Id.toString(), this)"
                };

                var config = new QueueSinkConfiguration()
                {
                    Name = "KafkaSinkTaskName", ConnectionStringName = "KafkaConStr", BrokerType = QueueBrokerType.Kafka, Scripts = { queueSinkScript }
                };

                var exception = Assert.Throws<LicenseLimitException>(() => store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config)));
                Assert.Equal(LimitType.QueueSink, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = Assert.Throws<LicenseLimitException>(() => store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config)));

                Assert.Equal(LimitType.QueueSink, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Raven_ETL()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);
                var connectionString = new RavenConnectionString
                {
                    Name = "RavenConnStr",
                    Database = store.Database,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                var config = new RavenEtlConfiguration
                {
                    Name = "RavenEtlTask",
                    ConnectionStringName = "RavenConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Users" },
                            Script = null
                        }
                    },
                    Disabled = true
                };
                var command = new AddRavenEtlCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Sql_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new SqlConnectionString
                {
                    Name = "SqlConnStr",
                    FactoryName = "System.Data.SqlClient",
                    ConnectionString = "Server=localhost;Database=Test;User Id=sa;Password=123456;"
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<SqlConnectionString>(connectionString));

                var config = new SqlEtlConfiguration
                {
                    Name = "SqlEtlTask",
                    ConnectionStringName = "SqlConnStr",
                    SqlTables = { new SqlEtlTable { TableName = "Users", DocumentIdColumn = "Id", InsertOnlyMode = false } },
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Users" },
                            Script = "loadToUsers(this)"
                        }
                    },
                    Disabled = true
                };

                var command = new AddSqlEtlCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Olap_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new OlapConnectionString
                {
                    Name = "OlapConnStr",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath(suffix: "OlapOutput")
                    }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<OlapConnectionString>(connectionString));

                var config = new OlapEtlConfiguration
                {
                    Name = "OlapEtlTask",
                    ConnectionStringName = "OlapConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "OlapTransform",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    },
                    Disabled = true
                };

                var command = new AddOlapEtlCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Elasticsearch_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new ElasticSearchConnectionString
                {
                    Name = "ElasticConnStr",
                    Nodes = new[] { "http://localhost:9200" }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<ElasticSearchConnectionString>(connectionString));

                var config = new ElasticSearchEtlConfiguration
                {
                    Name = "ElasticEtlTask",
                    ConnectionStringName = "ElasticConnStr",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "Script1",
                            Collections = new List<string> { "Products" },
                            Script = "loadToUsers(this)"
                        }
                    },
                    Disabled = true
                };
                var command = new AddElasticSearchEtlCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Queue_ETL()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new QueueConnectionString
                {
                    Name = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    KafkaConnectionSettings = new KafkaConnectionSettings
                    {
                        BootstrapServers = "localhost:9092"
                    }
                };

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(connectionString));

                var config = new QueueEtlConfiguration
                {
                    Name = "QueueEtlTask",
                    ConnectionStringName = "QueueConnStr",
                    BrokerType = QueueBrokerType.Kafka,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "QueueScript",
                            Collections = new List<string> { "Orders" },
                            Script = "loadToUsers(this)"
                        }
                    },
                    Disabled = true
                };

                var command = new AddQueueEtlCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task Put_Disabled_Downgrade_QueueSink()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(
                    new PutConnectionStringOperation<QueueConnectionString>(
                        new QueueConnectionString
                        {
                            Name = "KafkaConStr",
                            BrokerType = QueueBrokerType.Kafka,
                            KafkaConnectionSettings = new KafkaConnectionSettings() { BootstrapServers = "localhost:9092" }
                        }));

                QueueSinkScript queueSinkScript = new()
                {
                    Name = "orders",
                    Queues = new List<string>() { "orders" },
                    Script = @"this['@metadata']['@collection'] = 'Orders';
               put(this.Id.toString(), this)"
                };

                var config = new QueueSinkConfiguration() { ConnectionStringName = "KafkaConStr", BrokerType = QueueBrokerType.Kafka, Scripts = { queueSinkScript }, Disabled = true };

                store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(config));

                var command = new AddQueueSinkCommand(config, store.Database, RaftIdGenerator.NewId());
                await Server.ServerStore.SendToLeaderAsync(command);
            }
        }
        // ----------------------------------------
        // Tests for Replication License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_License_Downgrade_External_Replication()
        {
            DoNotReuseServer();
            using (var server1 = GetNewServer())
            {
                var options = new Options() { Server = server1 };
                using (var store1 = GetDocumentStore(options))
                using (var store2 = GetDocumentStore())
                {
                    await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                    {
                        Name = "ToTargetDb",
                        Database = store2.Database,
                        TopologyDiscoveryUrls = store2.Urls
                    }));

                    var replicationTask = new ExternalReplication { Name = "ReplicateToTargetDb", ConnectionStringName = "ToTargetDb", MentorNode = null };

                    await store1.Maintenance.SendAsync(
                        new UpdateExternalReplicationOperation(replicationTask));

                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "John Dow" }, "users/1");


                        session.SaveChanges();
                    }

                    var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 15000);
                    Assert.NotNull(replicated1);

                    await LicenseHelper.FailToChangeLicense(server1, LicenseTestBase.RL_COMM, LimitType.ExternalReplication);

                    await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(server1, store1, LicenseTestBase.RL_DEV);
                    await LicenseHelper.ChangeLicense(server1, LicenseTestBase.RL_PRO);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_Put_External_Replication()
        {
            DoNotReuseServer();
            using (var server1 = GetNewServer())
            {
                var options = new Options() { Server = server1 };
                using (var store1 = GetDocumentStore(options))
                using (var store2 = GetDocumentStore())
                {
                    await LicenseHelper.DisableRevisionCompression(server1, store1);
                    await LicenseHelper.DisableRevisionCompression(Server, store2);
                    await LicenseHelper.PutLicense(server1, LicenseTestBase.RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await SetupReplicationAsync(store1, store2));
                    Assert.Equal(LimitType.ExternalReplication, exception.LimitType);

                    await LicenseHelper.PutLicense(server1, LicenseTestBase.RL_PRO);
                    await SetupReplicationAsync(store1, store2);

                    await LicenseHelper.PutLicense(server1, LicenseTestBase.RL_DEV);
                    await SetupReplicationAsync(store1, store2);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_License_Downgrade_Pull_Replication_As_Hub()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("test"));
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.PullReplicationAsHub);

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.PullReplicationAsHub);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication) ]
        public async Task Prevent_Put_Pull_Replication_As_Hub_Replication()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("test")));
                Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("test")));
                Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);
            }
        }
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_Put_PullReplicationAsSink()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var connectionString = new RavenConnectionString
                {
                    Name = "HubConnStr",
                    Database = store.Database,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                var sink = new PullReplicationAsSink
                {
                    HubName = "aa",
                    ConnectionString = connectionString,
                    ConnectionStringName = connectionString.Name
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(sink)));

                Assert.Equal(LimitType.PullReplicationAsSink, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_License_Downgrade_Pull_Replication_As_Sink()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var config = new PullReplicationAsSink
                {
                    Name = "test-sink",
                    HubName = "hub1",
                    ConnectionStringName = "HubConnStr"
                };

                var connectionString = new RavenConnectionString
                {
                    Name = "HubConnStr",
                    Database = store.Database,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(config));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.PullReplicationAsSink);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_PRO);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_DEV);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_Put_Delayed_Replication()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var delayed = new ExternalReplication("otherDb", "DelayedReplication")
                {
                    DelayReplicationFor = TimeSpan.FromMinutes(5)
                };
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(delayed)));
                Assert.Equal(LimitType.ExternalReplication, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(delayed)));
                Assert.Equal(LimitType.DelayedExternalReplication, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(delayed));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task Prevent_License_Downgrade_Delayed_Replication()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var delayed = new ExternalReplication("otherDb", "DelayedReplication")
                {
                    DelayReplicationFor = TimeSpan.FromMinutes(5)
                };

                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(delayed));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.DelayedExternalReplication);
                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_PRO, LimitType.DelayedExternalReplication);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
            }
        }

        // ----------------------------------------
        // Tests for cluster size License Limits
        //  ----------------------------------------
        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task Prevent_License_Downgrade_Cluster_Size()
        {
            DoNotReuseServer();

            var (_, leader) = await CreateRaftCluster(6, false, watcherCluster: true);

            using (GetDocumentStore())
            {
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_COMM, LimitType.ClusterSize);
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_DEV, LimitType.ClusterSize);
                await LicenseHelper.FailToChangeLicense(leader, LicenseTestBase.RL_PRO, LimitType.ClusterSize);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task Prevent_Add_Node_To_Cluster()
        {
            DoNotReuseServer();

            var (_, leader) = await CreateRaftCluster(3, false, watcherCluster: true);

            using (GetDocumentStore())
            {
                await LicenseHelper.PutLicense(leader, LicenseTestBase.RL_COMM);
                using (var server = GetNewServer(new ServerCreationOptions()))
                using (var server2 = GetNewServer(new ServerCreationOptions()))
                using (var server3 = GetNewServer(new ServerCreationOptions()))
                {
                    var command = new AddClusterNodeCommand(server.WebUrl);
                    using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(leader.WebUrl, null, DocumentConventions.DefaultForServer))
                    using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await requestExecutor.ExecuteAsync(command, context));
                        Assert.Equal(LimitType.ClusterSize, exception.LimitType);

                        await LicenseHelper.PutLicense(leader, LicenseTestBase.RL_DEV);
                        exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await requestExecutor.ExecuteAsync(command, context));
                        Assert.Equal(LimitType.ClusterSize, exception.LimitType);

                        await LicenseHelper.PutLicense(leader, LicenseTestBase.RL_PRO);
                        await requestExecutor.ExecuteAsync(command, context); // cluster size = 4, which is allowed by PRO license
                        command = new AddClusterNodeCommand(server2.WebUrl);
                        await requestExecutor.ExecuteAsync(command, context); // cluster size = 5, which is allowed by PRO license
                        command = new AddClusterNodeCommand(server3.WebUrl);
                        exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await requestExecutor.ExecuteAsync(command, context));
                        Assert.Equal(LimitType.ClusterSize, exception.LimitType);
                    }
                }
            }
        }

        // ----------------------------------------
        // Tests for Time Series Configuration License Limits
        //  ----------------------------------------

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task Prevent_Put_TimeSeries_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var timeSeriesConfig = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        {
                            "Users", new TimeSeriesCollectionConfiguration
                            {
                                Policies = new List<TimeSeriesPolicy>
                                {
                                    new TimeSeriesPolicy("30Seconds", TimeValue.FromSeconds(30)),
                                    new TimeSeriesPolicy("1Hour", TimeValue.FromHours(1))
                                },
                                RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMinutes(1))
                            }
                        }
                    }
                };

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfig)));

                Assert.Equal(LimitType.TimeSeriesRollupsAndRetention, exception.LimitType);

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_PRO);
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfig));

                await LicenseHelper.PutLicense(Server, LicenseTestBase.RL_DEV);
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfig));
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task Prevent_License_Downgrade_TimeSeries_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                var timeSeriesConfig = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        {
                            "Users", new TimeSeriesCollectionConfiguration
                            {
                                Policies = new List<TimeSeriesPolicy>
                                {
                                    new TimeSeriesPolicy("30Seconds", TimeValue.FromSeconds(30)),
                                    new TimeSeriesPolicy("1Hour", TimeValue.FromHours(1))
                                },
                                RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMinutes(1))
                            }
                        }
                    }
                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfig));

                await LicenseHelper.FailToChangeLicense(Server, LicenseTestBase.RL_COMM, LimitType.TimeSeriesRollupsAndRetention);

                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_DEV);
                await LicenseHelper.ChangeLicense(Server, LicenseTestBase.RL_PRO);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task Put_Disabled_TimeSeries_Configuration()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.PutLicenseAndDisableRevisionCompression(Server, store, LicenseTestBase.RL_COMM);

                var timeSeriesConfig = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        {
                            "Users", new TimeSeriesCollectionConfiguration
                            {
                                Policies = new List<TimeSeriesPolicy>
                                {
                                    new TimeSeriesPolicy("30Seconds", TimeValue.FromSeconds(30)),
                                    new TimeSeriesPolicy("1Hour", TimeValue.FromHours(1))
                                },
                                RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMinutes(1)),
                                Disabled = true
                            }
                        }
                    }

                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfig));
            }
        }

        private static string GetSorter(string resourceName, string originalSorterName, string sorterName)
        {
            using (var stream = GetDump(resourceName))
            using (var reader = new StreamReader(stream))
            {
                var analyzerCode = reader.ReadToEnd();
                analyzerCode = analyzerCode.Replace(originalSorterName, sorterName);

                return analyzerCode;
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_8355).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
