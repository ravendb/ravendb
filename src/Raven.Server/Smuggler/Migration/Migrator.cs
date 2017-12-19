using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator
    {
        private const string MigrationStateKeyBase = "Raven/Migration/Status";
        
        private readonly HttpClient _client;
        private readonly string _serverUrl;
        private readonly ServerStore _serverStore;
        private readonly CancellationToken _cancellationToken;
        private MajorVersion _buildMajorVersion;
        private int _buildVersion;

        public Migrator(
            MigrationConfigurationBase configuration, 
            ServerStore serverStore,
            CancellationToken cancellationToken)
        {
            var hasCredentials =
                string.IsNullOrWhiteSpace(configuration.UserName) == false &&
                string.IsNullOrWhiteSpace(configuration.Password) == false;

            var httpClientHandler = new HttpClientHandler
            {
                UseDefaultCredentials = hasCredentials == false,
            };

            if (hasCredentials)
                httpClientHandler.Credentials = new NetworkCredential(
                    configuration.UserName,
                    configuration.Password,
                    configuration.Domain);

            _client = new HttpClient(httpClientHandler);

            _serverUrl = configuration.ServerUrl.TrimEnd('/');
            
            _serverStore = serverStore;
            _buildMajorVersion = configuration.BuildMajorVersion;
            _buildVersion = configuration.BuildVersion;
            _cancellationToken = cancellationToken;
        }

        public async Task UpdateBuildInfoIfNeeded()
        {
            if (_buildMajorVersion != MajorVersion.Unknown)
                return;

            var buildInfo = await GetBuildInfo();

            _buildMajorVersion = buildInfo.MajorVersion;
            _buildVersion = buildInfo.BuildVersion;

            if (buildInfo.MajorVersion == MajorVersion.Unknown)
            {
                throw new InvalidOperationException($"Unknown build version: {buildInfo.BuildVersion}, " +
                                                    $"product version: {buildInfo.ProductVersion}");
            }
        }

        public async Task<BuildInfo> GetBuildInfo()
        {
            var url = $"{_serverUrl}/build/version";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, _cancellationToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get build version from server: {_serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var buildInfo = await context.ReadForMemoryAsync(responseStream, "build-version-info");
                buildInfo.TryGet(nameof(BuildInfo.BuildVersion), out int buildVersion);
                buildInfo.TryGet(nameof(BuildInfo.ProductVersion), out string productVersion);
                buildInfo.TryGet(nameof(BuildInfo.FullVersion), out string fullVersion);

                MajorVersion version;
                if (buildVersion == 40 || buildVersion > 40000)
                {
                    version = MajorVersion.V4;
                }
                else if (buildVersion >= 35000)
                {
                    version = MajorVersion.V35;
                }
                else if (buildVersion >= 20000)
                {
                    version = MajorVersion.V2;
                }
                else if (buildVersion >= 3000)
                {
                    version = MajorVersion.V30;
                }
                else if (productVersion.StartsWith("2.") || buildVersion >= 2000)
                {
                    version = MajorVersion.V2;
                }
                else
                {
                    version = MajorVersion.Unknown;
                }

                return new BuildInfo
                {
                    ProductVersion = productVersion,
                    BuildVersion = buildVersion,
                    MajorVersion = version,
                    FullVersion = fullVersion
                };
            }
        }

        public async Task MigrateDatabases(List<string> databasesToMigrate)
        {
            await UpdateBuildInfoIfNeeded();

            if (databasesToMigrate == null || databasesToMigrate.Count == 0)
            {
                // migrate all databases
                databasesToMigrate = _buildMajorVersion == MajorVersion.V4
                    ? await Importer.GetDatabasesToMigrate(_serverUrl, _client, _cancellationToken)
                    : await AbstractLegacyMigrator.GetDatabasesToMigrate(_serverUrl, _client, _cancellationToken);
            }

            if (databasesToMigrate.Count == 0)
                throw new InvalidOperationException("Found no databases to migrate");

            _serverStore.EnsureNotPassive();

            foreach (var databaseNameToMigrate in databasesToMigrate)
            {
                await CreateDatabaseIfNeeded(databaseNameToMigrate);
                var database = await GetDatabase(databaseNameToMigrate);
                if (database == null)
                {
                    // database doesn't exist
                    continue;
                }

                StartMigratingSingleDatabase(databaseNameToMigrate, database);
            }
        }

        public long StartMigratingSingleDatabase(string sourceDatabaseName, DocumentDatabase database)
        {
            var operationId = database.Operations.GetNextOperationId();
            var cancelToken = new OperationCancelToken(_cancellationToken);
            var result = new SmugglerResult();

            database.Operations.AddOperation(null,
                $"Database name: '{sourceDatabaseName}' from url: {_serverUrl}",
                Operations.OperationType.DatabaseMigration,
                taskFactory: onProgress => Task.Run(async () =>
                {
                    onProgress?.Invoke(result.Progress);

                    var majorVersion = _buildMajorVersion;
                    var message = $"Importing from RavenDB {EnumHelper.GetDescription(majorVersion)}";

                    result.AddInfo(message);

                    using (cancelToken)
                    {
                        try
                        {
                            var migrationStateKey = $"{MigrationStateKeyBase}/" +
                                                 $"{EnumHelper.GetDescription(majorVersion)}/" +
                                                 $"{sourceDatabaseName}/" +
                                                 $"{_serverUrl}";

                            AbstractMigrator migrator;
                            switch (majorVersion)
                            {
                                case MajorVersion.V2:
                                    migrator = new Migrator_V2(migrationStateKey, _serverUrl, sourceDatabaseName, 
                                        result, onProgress, database, _client, cancelToken);
                                    break;
                                case MajorVersion.V30:
                                case MajorVersion.V35:
                                    migrator = new Migrator_V3(migrationStateKey, _serverUrl, sourceDatabaseName, 
                                        result, onProgress, database, _client, majorVersion, _buildVersion, cancelToken);
                                    break;
                                case MajorVersion.V4:
                                    migrator = new Importer(migrationStateKey, _serverUrl, sourceDatabaseName, result, onProgress, database, cancelToken);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(majorVersion), majorVersion, null);
                            }

                            using (migrator)
                            {
                                await migrator.Execute();
                            }
                        }
                        catch (Exception e)
                        {
                            result.AddError($"Error occurred during database migration named: {sourceDatabaseName}." +
                                            $"Exception: {e.Message}");

                            throw;
                        }
                    }

                    return (IOperationResult)result;
                }, cancelToken.Token), id: operationId, token: cancelToken);

            return operationId;
        }

        private async Task CreateDatabaseIfNeeded(string databaseName)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = _serverStore.Cluster.ReadDatabase(context, databaseName);
                if (record != null)
                {
                    if (record.Topology.AllNodes.Contains(_serverStore.NodeTag) == false)
                        throw new InvalidOperationException(
                            "Cannot migrate database because " +
                            $"database doesn't exist on the current node ({_serverStore.NodeTag})");

                    // database already exists
                    return;
                }

                var databaseRecord = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        Members =
                        {
                            _serverStore.NodeTag
                        }
                    }
                };

                var (index, _) = await _serverStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null);
                await _serverStore.Cluster.WaitForIndexNotification(index);
            }
        }

        private async Task<DocumentDatabase> GetDatabase(string databaseName)
        {
            return await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
        }
    }
}
