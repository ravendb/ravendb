using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
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
        private MajorVersion _version;
        private int _buildVersion;
        private string _fullVersion;

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
            _version = configuration.MajorVersion;
            _cancellationToken = cancellationToken;
        }

        public async Task UpdateSourceServerVersion()
        {
            var buildInfo = await GetBuildInfo();
            _buildVersion = buildInfo.BuildVersion;
            _version = buildInfo.MajorVersion;
            _fullVersion = buildInfo.FullVersion;

            if (_version == MajorVersion.Unknown)
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
            if (databasesToMigrate == null || databasesToMigrate.Count == 0)
            {
                // migrate all databases
                databasesToMigrate = await GetDatabasesToMigrate();
            }

            if (databasesToMigrate.Count == 0)
                throw new InvalidOperationException("Found no databases to migrate");

            _serverStore.EnsureNotPassive();

            await UpdateSourceServerVersion();

            foreach (var databaseNameToMigrate in databasesToMigrate)
            {
                await CreateDatabaseIfNeeded(databaseNameToMigrate);
                var database = await GetDatabase(databaseNameToMigrate);
                if (database == null)
                {
                    // database doesn't exist
                    continue;
                }

                StartMigrateSingleDatabase(databaseNameToMigrate, database);
            }
        }

        public long StartMigrateSingleDatabase(string sourceDatabaseName, DocumentDatabase database)
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

                    var message = $"Importing from RavenDB {GetDescription(_version)}, " +
                                  $"build version: {_buildVersion}";

                    if (string.IsNullOrWhiteSpace(_fullVersion) == false)
                        message += $", full version: {_fullVersion}";

                    result.AddMessage(message);

                    using (cancelToken)
                    {
                        try
                        {
                            var migrationStateKey = $"{MigrationStateKeyBase}/" +
                                                 $"{GetDescription(_version)}/" +
                                                 $"{sourceDatabaseName}/" +
                                                 $"{_serverUrl}";

                            AbstractMigrator migrator;
                            switch (_version)
                            {
                                case MajorVersion.V2:
                                    migrator = new Migrator_V2(_serverUrl, sourceDatabaseName, result, onProgress, database, _client, cancelToken);
                                    break;
                                case MajorVersion.V30:
                                case MajorVersion.V35:
                                    migrator = new Migrator_V3(_serverUrl, sourceDatabaseName, result, onProgress, 
                                        database, _client, migrationStateKey, _version, cancelToken);
                                    break;
                                case MajorVersion.V4:
                                    migrator = new Importer(_serverUrl, sourceDatabaseName, result, onProgress, database, migrationStateKey, cancelToken);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(_version), _version, null);
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

        public static string GetDescription(Enum value)
        {
            var fi = value.GetType().GetField(value.ToString());

            if (fi != null)
            {
                var attributes = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
                return (attributes.Length > 0) ? attributes[0].Description : value.ToString();
            }

            return value.ToString();
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

        private async Task<List<string>> GetDatabasesToMigrate()
        {
            var url = $"{_serverUrl}/databases";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, _cancellationToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get databases to migrate from server: {_serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var responseStream = await response.Content.ReadAsStreamAsync();
                using (var reader = new StreamReader(responseStream, Encoding.UTF8))
                {
                    var jsonStr = reader.ReadToEnd();
                    return JsonConvert.DeserializeObject<List<string>>(jsonStr);
                }
            }
        }
    }
}
