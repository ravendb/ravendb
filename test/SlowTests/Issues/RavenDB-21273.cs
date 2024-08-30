using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
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

        [MultiLicenseRequiredFact]
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
                    await ChangeLicense(Server, RL_COMM);
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

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingSnapshotWithCommunityLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Snapshot backups feature"));
                }
            }
            finally
            {
                File.Delete(file);
            }


        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingSnapshotWithProLicense()
        {
            DoNotReuseServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", backupType: BackupType.Snapshot);
                    await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));



                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_PRO);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Snapshot backups feature"));
                }
            }
            finally
            {
                File.Delete(file);
            }


        }


        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingExternalReplicationWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                using (var store = GetDocumentStore())
                {
                    var connectionString = new RavenConnectionString
                    {
                        Name = csName,
                        Database = dbName,
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                    };

                    var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                    Assert.NotNull(result.RaftCommandIndex);

                    var watcher = new ExternalReplication(dbName, csName);
                    await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding External Replication."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingDelayedExternalReplicationWithProLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                using (var store = GetDocumentStore())
                {
                    var connectionString = new RavenConnectionString
                    {
                        Name = csName,
                        Database = dbName,
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                    };

                    var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                    Assert.NotNull(result.RaftCommandIndex);

                    var watcher = new ExternalReplication(dbName, csName);
                    await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding External Replication."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
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
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Time Series Rollups And Retention feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
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
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
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
                    await ChangeLicense(Server, RL_PRO);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Documents Compression feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingPullReplicationAsSinkWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            try
            {
                using (var store = GetDocumentStore())
                {
                    var pullAsSink = new PullReplicationAsSink(dbName, csName, "hub");
                    var result = await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullAsSink));
                    Assert.NotNull(result.RaftCommandIndex);

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Sink Replication feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithCommunityLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    store.Maintenance.Send(new PutPullReplicationAsHubOperation("sink"));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Hub Replication feature."));
                    WaitForUserToContinueTheTest(store);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingPullReplicationAsHubWithProLicense()
        {
            DoNotReuseServer();

            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    store.Maintenance.Send(new PutPullReplicationAsHubOperation("sink"));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_PRO);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Hub Replication feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [MultiLicenseRequiredFact]
        public async Task ExceptionWhenImportingRavenEtlWithCommunityLicense()
        {
            DoNotReuseServer();
            var dbName = $"db/{Guid.NewGuid()}";
            var csName = $"cs/{Guid.NewGuid()}";
            var file = GetTempFileName();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var etlConfiguration = new RavenEtlConfiguration
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
                    store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var store = GetDocumentStore())
                {
                    await ChangeLicense(Server, RL_COMM);

                    var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                    {
                        var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    });
                    Assert.Equal(LimitType.InvalidLicense, exception.Type);
                    Assert.True(exception.Message.Contains("Your license doesn't support adding Raven ETL feature."));
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

      
        private static async Task ChangeLicense(RavenServer server, string licenseType)
        {
            var license = Environment.GetEnvironmentVariable(licenseType);
            LicenseHelper.TryDeserializeLicense(license, out License li);

            await server.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId());
        }

    }
}
