using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Commands;
using Sparrow;
using Xunit;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public readonly LicenseTestBase LicenseHelper;

        public class LicenseTestBase
        {
            private readonly RavenTestBase _parent;

            public LicenseTestBase(RavenTestBase parent)
            {
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            }

            internal const string RL_COMM = "RAVEN_LICENSE_COMMUNITY";
            internal const string RL_PRO = "RAVEN_LICENSE_PROFESSIONAL";
            internal const string RL_DEV = "RAVEN_LICENSE_DEVELOPER";

            internal async Task FailToChangeLicense(RavenServer leader, string licenseType, LimitType limitType)
            {
                var license = Environment.GetEnvironmentVariable(licenseType);
                Raven.Server.Commercial.LicenseHelper.TryDeserializeLicense(license, out License li);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () => await leader.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId()));

                Assert.Equal(limitType, exception.LimitType);
            }

            internal async Task ChangeLicense(RavenServer leader, string licenseType)
            {
                var license = Environment.GetEnvironmentVariable(licenseType);
                Raven.Server.Commercial.LicenseHelper.TryDeserializeLicense(license, out License li);

                await leader.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId());
            }

            internal async Task PutLicense(RavenServer leader, string licenseType)
            {
                var license = Environment.GetEnvironmentVariable(licenseType);
                Raven.Server.Commercial.LicenseHelper.TryDeserializeLicense(license, out License li);

                await leader.ServerStore.PutLicenseAsync(li, RaftIdGenerator.NewId());
            }

            internal async Task DisableRevisionCompression(RavenServer leader, DocumentStore store)
            {
                var command = new EditDocumentsCompressionCommand(new DocumentsCompressionConfiguration { CompressRevisions = false, Collections = new string[] { } }, store.Database,
                    RaftIdGenerator.NewId());
                await leader.ServerStore.SendToLeaderAsync(command);
            }

            internal async Task PutLicenseAndDisableRevisionCompression(RavenServer server, DocumentStore store, string licenseType)
            {
                await DisableRevisionCompression(server, store);
                await PutLicense(server, licenseType);
            }

            internal async Task ChangeLicenseAndDisableRevisionCompression(RavenServer server, DocumentStore store, string licenseType)
            {
                await DisableRevisionCompression(server, store);
                await ChangeLicense(server, licenseType);
            }

            internal void PutIndexWithAdditionalAssemblies(DocumentStore store, IndexState indexState = IndexState.Normal)
            {
                store.Maintenance.Send(new PutIndexesOperation(new[]
                {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs.Images select new { doc.Tags }" },
                        Name = "test",
                        AdditionalAssemblies = { AdditionalAssembly.FromNuGet("System.Drawing.Common", "4.7.0") },
                        State = indexState
                    }
                }));
            }

            internal async Task<PeriodicBackupConfiguration> CreatePeriodicBackup(string backupPath, DocumentStore store ,BackupType type, bool disabled = false)
            {
                PeriodicBackupConfiguration config = new PeriodicBackupConfiguration()
                {
                    BackupType = type,
                    FullBackupFrequency = "* */1 * * *",
                    Disabled = disabled,
                    BackupUploadMode = BackupUploadMode.Default,
                    PinToMentorNode = false,
                    LocalSettings = new LocalSettings { FolderPath = backupPath }
                };
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                return config;
            }

            internal async Task<ExternalReplication> CreateExternalReplication(string csName, string dbName, DocumentStore store, bool disabled = false)
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
                watcher.Disabled = disabled;
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(watcher));
                return watcher;
            }

            internal void CreateTsRollupAndRetention(DocumentStore store, bool disabled = false)
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
                salesTsConfig.Disabled = disabled;
                databaseTsConfig.Collections["Sales"] = salesTsConfig;
                store.Maintenance.Send(new ConfigureTimeSeriesOperation(databaseTsConfig));
            }

            internal void CreateCompressAllCollection(DocumentStore store)
            {
                var dbrecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                dbrecord.DocumentsCompression.CompressAllCollections = true;
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(dbrecord, dbrecord.Etag));
            }

            internal async Task<PullReplicationAsSink> CreatePullReplicationAsSink(string dbName, string csName, DocumentStore store, bool disabled = false)
            {
                PullReplicationAsSink pullAsSink;
                pullAsSink = new PullReplicationAsSink(dbName, csName, "hub");
                pullAsSink.Disabled = disabled;
                var result = await store.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullAsSink));
                Assert.NotNull(result.RaftCommandIndex);
                return pullAsSink;
            }

            internal  PullReplicationDefinition CraetePullReplicationDefinition(DocumentStore store, bool disabled = false)
            {
                PullReplicationDefinition pull;
                pull = new PullReplicationDefinition("pull");
                pull.Disabled = disabled;
                store.Maintenance.Send(new PutPullReplicationAsHubOperation(pull));
                return pull;
            }

            internal RavenEtlConfiguration CreateRavenEtlConfiguration(string csName, string dbName, DocumentStore store, out AddEtlOperationResult etl, bool disabled = false)
            {
                RavenEtlConfiguration etlConfiguration;
                etlConfiguration = new RavenEtlConfiguration
                {
                    Name = csName,
                    ConnectionStringName = csName,
                    Transforms = { new Transformation { Name = $"ETL : {csName}", ApplyToAllDocuments = true } },
                    MentorNode = "A",
                    Disabled = disabled
                };
                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" },
                };

                Assert.NotNull(store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString)));
                etl = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
                return etlConfiguration;
            }

            internal async Task CreateSqlEtlConfiguration(DocumentStore store, bool disabled = false)
            {
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
                    Disabled = disabled
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<SqlConnectionString>(config));
            }
        }
    }
}
