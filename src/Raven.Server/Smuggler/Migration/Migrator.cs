using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration.ApiKey;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator
    {
        public const string MigrationStateKeyBase = "Raven/Migration/Status/";

        private readonly string _serverUrl;
        private readonly HttpClient _httpClient;
        private readonly ServerStore _serverStore;
        private readonly string _apiKey;
        private readonly bool _enableBasicAuthenticationOverUnsecuredHttp;
        private readonly bool _skipServerCertificateValidation;
        private MajorVersion _buildMajorVersion;
        private int _buildVersion;

        public Migrator(MigrationConfigurationBase configuration, ServerStore serverStore)
        {
            _serverUrl = UrlHelper.TryGetLeftPart(configuration.ServerUrl);
            _serverStore = serverStore;
            _buildMajorVersion = configuration.BuildMajorVersion;
            _buildVersion = configuration.BuildVersion;
            _skipServerCertificateValidation = configuration.SkipServerCertificateValidation;

            //because of backward compatibility useCompression == false here
            var httpClientHandler = RequestExecutor.CreateHttpMessageHandler(_serverStore.Server.Certificate.Certificate, setSslProtocols: false, useCompression: false);
            httpClientHandler.UseDefaultCredentials = false;

            if (configuration.SkipServerCertificateValidation)
                httpClientHandler.ServerCertificateCustomValidationCallback = (message, certificate2, arg3, arg4) => true;

            if (string.IsNullOrWhiteSpace(configuration.ApiKey) == false)
            {
                Authenticator.GetApiKeyParts(configuration.ApiKey); // will throw if not in correct format
                _apiKey = configuration.ApiKey;
                _enableBasicAuthenticationOverUnsecuredHttp = configuration.EnableBasicAuthenticationOverUnsecuredHttp;
            }
            else if (string.IsNullOrWhiteSpace(configuration.UserName) == false &&
                string.IsNullOrWhiteSpace(configuration.Password) == false)
            {
                var domain = string.IsNullOrWhiteSpace(configuration.Domain) ? "\\" : configuration.Domain;
                httpClientHandler.Credentials = new NetworkCredential(
                    configuration.UserName,
                    configuration.Password,
                    domain);
            }

            _httpClient = new HttpClient(httpClientHandler);
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

            var response = await _httpClient.SendAsync(request, _serverStore.ServerShutdown);
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
                if ((buildVersion >= 40 && buildVersion <= 49) || buildVersion > 40000)
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

        public async Task MigrateDatabases(List<DatabaseMigrationSettings> databases)
        {
            await UpdateBuildInfoIfNeeded();

            if (databases == null || databases.Count == 0)
            {
                // migrate all databases
                var authorized = new Reference<bool>();
                var databaseNames = await GetDatabaseNames(_buildMajorVersion, authorized);
                if (authorized.Value == false)
                    throw new InvalidOperationException($"You are not authorized to fetch " +
                                                        $"the database names for server: {_serverUrl}");

                if (databases == null)
                    databases = new List<DatabaseMigrationSettings>();

                var operateOnTypes = DatabaseSmugglerOptions.DefaultOperateOnTypes;
                if (_buildMajorVersion != MajorVersion.V4)
                {
                    operateOnTypes |= DatabaseItemType.LegacyAttachments;
                }

                foreach (var databaseName in databaseNames)
                {
                    databases.Add(new DatabaseMigrationSettings
                    {
                        DatabaseName = databaseName,
                        OperateOnTypes = operateOnTypes,
                        RemoveAnalyzers = false
                    });
                }
            }

            if (databases.Count == 0)
                throw new InvalidOperationException("Found no databases to migrate");

            _serverStore.EnsureNotPassive();

            foreach (var databaseToMigrate in databases)
            {
                await CreateDatabaseIfNeeded(databaseToMigrate.DatabaseName);
                var database = await GetDatabase(databaseToMigrate.DatabaseName);
                if (database == null)
                {
                    // database doesn't exist
                    continue;
                }

                StartMigratingSingleDatabase(databaseToMigrate, database);
            }
        }

        public async Task<List<string>> GetFileSystemNames(MajorVersion buildMajorVersion)
        {
            if (buildMajorVersion != MajorVersion.V30 && buildMajorVersion != MajorVersion.V35)
                return new List<string>();

            try
            {
                return await AbstractLegacyMigrator.GetResourcesToMigrate(_serverUrl, _httpClient, true, _apiKey, _enableBasicAuthenticationOverUnsecuredHttp, _skipServerCertificateValidation, null, _serverStore.ServerShutdown);
            }
            catch (Exception e) when (e is UnauthorizedAccessException || e is AuthorizationException)
            {
                return new List<string>();
            }
        }

        public async Task<List<string>> GetDatabaseNames(MajorVersion buildMajorVersion, Reference<bool> authorized, Reference<bool> isLegacyOAuthToken = null)
        {
            authorized.Value = true;
            if (buildMajorVersion == MajorVersion.Unknown)
                return new List<string>();

            try
            {
                return buildMajorVersion == MajorVersion.V4
                    ? await Importer.GetDatabasesToMigrate(_serverUrl, _httpClient, _serverStore.ServerShutdown)
                    : await AbstractLegacyMigrator.GetResourcesToMigrate(_serverUrl, _httpClient, false, _apiKey, _enableBasicAuthenticationOverUnsecuredHttp, _skipServerCertificateValidation, isLegacyOAuthToken, _serverStore.ServerShutdown);
            }
            catch (Exception e) when (e is UnauthorizedAccessException || e is AuthorizationException)
            {
                authorized.Value = false;
                return new List<string>();
            }
        }

        public long StartMigratingSingleDatabase(DatabaseMigrationSettings databaseMigrationSettings, DocumentDatabase database)
        {
            var operationId = database.Operations.GetNextOperationId();
            var cancelToken = new OperationCancelToken(database.DatabaseShutdown);
            var result = new SmugglerResult();

            var databaseName = databaseMigrationSettings.DatabaseName;
            database.Operations.AddOperation(null,
                $"Database name: '{databaseName}' from url: {_serverUrl}",
                Operations.OperationType.DatabaseMigrationRavenDb,
                taskFactory: onProgress => Task.Run(async () =>
                {
                    onProgress?.Invoke(result.Progress);

                    var message = $"Importing from RavenDB {EnumHelper.GetDescription(_buildMajorVersion)}";

                    result.AddInfo(message);

                    using (cancelToken)
                    {
                        try
                        {
                            var migrationStateKey = $"{MigrationStateKeyBase}" +
                                                    $"{EnumHelper.GetDescription(_buildMajorVersion)}/" +
                                                    $"{databaseName}/" +
                                                    $"{_serverUrl}";

                            var options = new MigratorOptions
                            {
                                MigrationStateKey = migrationStateKey,
                                ServerUrl = _serverUrl,
                                DatabaseName = databaseName,
                                ApiKey = _apiKey,
                                TransformScript = databaseMigrationSettings.TransformScript,
                                EnableBasicAuthenticationOverUnsecuredHttp = _enableBasicAuthenticationOverUnsecuredHttp,
                                SkipServerCertificateValidation = _skipServerCertificateValidation,
                                RemoveAnalyzers = databaseMigrationSettings.RemoveAnalyzers,
                                ImportRavenFs = databaseMigrationSettings.ImportRavenFs,
                                OperateOnTypes = databaseMigrationSettings.OperateOnTypes
                            };

                            var parameters = new MigratorParameters
                            {
                                HttpClient = _httpClient,
                                Result = result,
                                OnProgress = onProgress,
                                Database = database,
                                CancelToken = cancelToken
                            };

                            AbstractMigrator migrator;
                            switch (_buildMajorVersion)
                            {
                                case MajorVersion.V2:
                                    migrator = new Migrator_V2(options, parameters);
                                    break;
                                case MajorVersion.V30:
                                case MajorVersion.V35:
                                    migrator = new Migrator_V3(options, parameters, _buildMajorVersion, _buildVersion);
                                    break;
                                case MajorVersion.V4:
                                    migrator = new Importer(options, parameters, _buildVersion);
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(_buildMajorVersion), _buildMajorVersion, null);
                            }

                            await migrator.Execute();
                        }
                        catch (Exception e)
                        {
                            result.AddError($"Error occurred during database migration named: {databaseName}." +
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
