using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Commands;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21273 : RavenTestBase
    {
        private const string RL_COMM = "RAVEN_LICENSE_COMMUNITY";
        private const string RL_PRO = "RAVEN_LICENSE_PROFESSIONAL";

        public RavenDB_21273(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Smuggler | RavenTestCategory.Compression)]
        public async Task ExceptionWhenImportingAdditionalAssembliesWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var dbrecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                    dbrecord.DocumentsCompression.CompressRevisions = false;
                    store.Maintenance.Server.Send(new UpdateDatabaseOperation(dbrecord, dbrecord.Etag));

                    store.Maintenance.Send(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition
                        {
                            Maps = { "from doc in docs.Images select new { doc.Tags }" },
                            Name = "test",
                            AdditionalAssemblies = { AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0") }
                        }
                    }));
                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var index = store.Maintenance.Send(new GetIndexOperation("test"));
                    Assert.Null(index);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ExceptionWhenImportingSnapshotWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    config.Disabled = false;
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        await Backup.UpdateConfigAsync(Server, config, store);
                    });
                    Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }


        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.BackupExportImport)]
        public async Task ExceptionWhenImportingSnapshotWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                PeriodicBackupConfiguration config;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_PRO, store);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    config.Disabled = false;
                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        await Backup.UpdateConfigAsync(Server, config, store);

                    });
                    Assert.Equal(LimitType.SnapshotBackup, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }


        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingExternalReplicationWithCommunityLicense()
        {
            DoNotReuseServer();
            using (var server = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var file = GetTempFileName();
                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server }))
                    {
                        var command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store2.Database,
                            RaftIdGenerator.NewId());
                        await server.ServerStore.SendToLeaderAsync(command);
                        command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store1.Database,
                            RaftIdGenerator.NewId());
                        await server2.ServerStore.SendToLeaderAsync(command);

                        var connectionString = new RavenConnectionString
                        {
                            Name = csName,
                            Database = dbName,
                            TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                        };

                        var result = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                        Assert.NotNull(result.RaftCommandIndex);

                        var watcher = new ExternalReplication(dbName, csName);
                        await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await ChangeLicense(server, RL_COMM, store2);
                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                        var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                        {
                            watcher.Disabled = false;
                            await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));
                        });
                        Assert.Equal(LimitType.ExternalReplication, exception.LimitType);
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingDelayedExternalReplicationWithProLicense()
        {
            DoNotReuseServer();
            using (var server = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var file = GetTempFileName();
                var dbName = $"cs/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";
                try
                {
                    using (var store1 = GetDocumentStore(new Options() { Server = server2 }))
                    using (var store2 = GetDocumentStore(new Options() { Server = server }))
                    {
                        var command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store2.Database,
                            RaftIdGenerator.NewId());
                        await server.ServerStore.SendToLeaderAsync(command);
                        command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store1.Database,
                            RaftIdGenerator.NewId());
                        await server2.ServerStore.SendToLeaderAsync(command);

                        var connectionString = new RavenConnectionString { Name = csName, Database = dbName, TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" } };

                        var result = await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                        Assert.NotNull(result.RaftCommandIndex);

                        var watcher = new ExternalReplication(dbName, csName);
                        await store1.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await ChangeLicense(server, RL_COMM, store2);

                        var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                        var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                        {
                            watcher.Disabled = false;
                            await store2.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));
                        });
                        Assert.Equal(LimitType.ExternalReplication, exception.LimitType);
                    }
                }
                finally
                {
                    File.Delete(file);
                }
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.TimeSeries)]
        public async Task ExceptionWhenImportingTsRollupAndRetentionWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var salesTsConfig = new TimeSeriesCollectionConfiguration
                    {
                        Policies = new List<TimeSeriesPolicy>
                        {
                            new("DailyRollupForOneYear",
                                TimeValue.FromDays(1),
                                TimeValue.FromYears(1))
                },
                        RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromDays(7))
                    };
                    var databaseTsConfig = new TimeSeriesConfiguration();
                    databaseTsConfig.Collections["Sales"] = salesTsConfig;
                    store.Maintenance.Send(new ConfigureTimeSeriesOperation(databaseTsConfig));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.TimeSeriesRollupsAndRetention, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Time Series Rollups And Retention feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Compression)]
        public async Task ExceptionWhenImportingCompressionWithCommunityLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var dbrecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                    dbrecord.DocumentsCompression.CompressAllCollections = true;
                    store.Maintenance.Server.Send(new UpdateDatabaseOperation(dbrecord, dbrecord.Etag));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Compression)]
        public async Task ExceptionWhenImportingCompressionWithProLicense()
        {
            DoNotReuseServer();
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var dbrecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                    dbrecord.DocumentsCompression.CompressAllCollections = true;
                    store.Maintenance.Server.Send(new UpdateDatabaseOperation(dbrecord, dbrecord.Etag));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_PRO, store);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.DocumentsCompression, exception.LimitType);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsSinkWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                PullReplicationAsSink pullAsSink;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    pullAsSink = new PullReplicationAsSink(dbName, csName, "hub");
                    var result = await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullAsSink));
                    Assert.NotNull(result.RaftCommandIndex);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        pullAsSink.Disabled = false;
                        await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullAsSink));
                    });
                    Assert.Equal(LimitType.PullReplicationAsSink, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                PullReplicationDefinition pull;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    pull = new PullReplicationDefinition("pull");
                    store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    pull.Disabled = false;
                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));
                    });
                    Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithProLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                PullReplicationDefinition pull;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    pull = new PullReplicationDefinition("pull");
                    store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_PRO, store);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    pull.Disabled = false;
                    var exception = Assert.Throws<LicenseLimitException>(() =>
                    {
                        store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));
                    });
                    Assert.Equal(LimitType.PullReplicationAsHub, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Etl)]
        public async Task ExceptionWhenImportingRavenEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            var file = GetTempFileName();
            try
            {
                AddEtlOperationResult etl;
                RavenEtlConfiguration etlConfiguration;
                using (var store = GetDocumentStore())
                {
                    await DisableRevisionCompression(Server, store.Database);
                    etlConfiguration = new RavenEtlConfiguration
                    {
                        Name = csName,
                        ConnectionStringName = csName,
                        Transforms = { new Transformation { Name = $"ETL : {csName}", ApplyToAllDocuments = true } },
                        MentorNode = "A",
                    };
                    var connectionString = new RavenConnectionString
                    {
                        Name = csName,
                        Database = dbName,
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" },
                    };

                    Assert.NotNull(store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString)));
                    etl = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM, store);

                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    etlConfiguration.Disabled = false;
                    var op = new UpdateEtlOperation<RavenConnectionString>(etl.TaskId, etlConfiguration);

                    var exception = Assert.Throws<LicenseLimitException>( () =>
                    {
                        store.Maintenance.Send(op);
                    });
                    Assert.Equal(LimitType.RavenEtl, exception.LimitType);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }


        private static async Task ChangeLicense(RavenServer server, string licenseType, DocumentStore store)
        {
            var license = Environment.GetEnvironmentVariable(licenseType);
            LicenseHelper.TryDeserializeLicense(license, out License li);
            await RavenDB_21427.DisableRevisionCompression(server, store);
            await server.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId());
        }

        internal static async Task DisableRevisionCompression(RavenServer leader, string name)
        {
            var command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, name,
                RaftIdGenerator.NewId());
            await leader.ServerStore.SendToLeaderAsync(command);
        }
    }
}
