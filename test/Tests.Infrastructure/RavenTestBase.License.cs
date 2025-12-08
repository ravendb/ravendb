using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
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
using Xunit;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public readonly LicenseTestBase LicenseHelper;

        public class LicenseTestBase
        {
            private readonly RavenTestBase _parent;
            internal string DefaultGenAiTaskName = "localGenAiTask";
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

            internal async Task RunImport(RavenServer server, DocumentStore store, string file)
            {
                await ChangeLicenseAndDisableRevisionCompression(server, store, RL_COMM);
                var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
            }

            internal async Task<LicenseLimitException> FailedToDoImport(RavenServer server, DocumentStore store, string file)
            {
                await ChangeLicenseAndDisableRevisionCompression(server, store, RL_COMM);
                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                });
                return exception;
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

            internal void RunRestore(DocumentStore store, string backupPath)
            {
                var configuration = new RestoreBackupConfiguration { DatabaseName = store.Database + "1" };
                configuration.BackupLocation = Directory.GetDirectories(backupPath).First();
                _parent.Backup.RestoreDatabase(store, configuration);
            }

            internal async Task RunBackup(DocumentStore store, string backupPath)
            {
                var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                _ = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            }

            internal AiConnectionString GetConnectionString()
            {
                var connectionString = new AiConnectionString
                {
                    Name = "test-connection",
                    ModelType = AiModelType.Chat,
                    OpenAiSettings = new OpenAiSettings { ApiKey = "test-key", Model = "gpt-4", Endpoint = "https://google.com/v5" }
                };
                connectionString.Identifier = connectionString.GenerateIdentifier();
                return connectionString;
            }

            internal AiAgentConfiguration AiAgentConfig(AiConnectionString connectionString)
            {
                var agent = new AiAgentConfiguration("shopping-assistant", connectionString.Name,
                    "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.");
                agent.Identifier = "shopping-assistant";
                agent.Parameters.Add(new AiAgentParameter("company", "The company ID"));
                agent.SampleObject = JsonConvert.SerializeObject(new { answer = "string" });
                agent.Queries =
                [
                    new AiAgentToolQuery
                    {
                        Name = "ProductSearch",
                        Description = "semantic search the store product catalog",
                        Query = "from Products where vector.search(embedding.text(Name), $query)",
                        ParametersSampleObject = "{\"query\": [\"term or phrase to search in the catalog\"]}"
                    },
                    new AiAgentToolQuery
                    {
                        Name = "RecentOrder",
                        Description = "Get the recent orders of the current user",
                        Query = "from Orders where Company = $company order by OrderedAt desc limit 10",
                        ParametersSampleObject = "{}"
                    }
                ];

                return agent;
            }

            internal async Task AddAiAgentIntegration(DocumentStore store, bool disabled = false)
            {
                AiConnectionString connectionString = GetConnectionString();

                var configuration = AiAgentConfig(connectionString);
                configuration.ConnectionStringName = connectionString.Name;
                configuration.Disabled = disabled;

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(connectionString));
                await store.Maintenance.SendAsync(new AddOrUpdateAiAgentOperation(configuration));
            }

            internal async Task AddGenAiIntegration(DocumentStore store, bool disabled = false)
            {
                AiConnectionString connectionString = GetConnectionString();

                var configuration = GenAiConfig(disabled);
                configuration.ConnectionStringName = connectionString.Name;
                configuration.Identifier = configuration.GenerateIdentifier();

                await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(connectionString));
                await store.Maintenance.SendAsync(new AddGenAiOperation(configuration));
            }

            internal GenAiConfiguration GenAiConfig(bool disabled)
            {
                return new GenAiConfiguration
                {
                    Name = DefaultGenAiTaskName,
                    Collection = "TestCollection",
                    Prompt = "Test prompt",
                    SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" }),
                    UpdateScript = @"
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}",
                    GenAiTransformation = new GenAiTransformation
                    {
                        Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                    },
                    Disabled = disabled
                };
            }
        }
    }
}
