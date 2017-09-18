using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Web.System;
using Sparrow.Json;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator
    {
        private const string MigrationStateKeyBase = "Raven/Migration/Status";

        private readonly HttpClient _client;
        private readonly string _serverUrl;
        private readonly string _migrationStateKey;
        private readonly ServerStore _serverStore;
        private readonly Operations _operations;
        private readonly CancellationToken _cancellationToken;

        public Migrator(
            MigrationConfigurationBase configuration, 
            ServerStore serverStore,
            Operations operations,
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
            _migrationStateKey = $"{MigrationStateKeyBase}/{_serverUrl}";
            _serverStore = serverStore;
            _operations = operations;
            _cancellationToken = cancellationToken;
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
            var operationId = _operations.GetNextOperationId();
            var cancelToken = new OperationCancelToken(_cancellationToken);
            var result = new SmugglerResult();

            _operations.AddOperation(null,
                $"Database name: '{sourceDatabaseName}' from url: {_serverUrl}",
                Operations.OperationType.DatabaseMigration,
                taskFactory: onProgress => Task.Run(async () =>
                {
                    onProgress?.Invoke(result.Progress);

                    using (cancelToken)
                    {
                        try
                        {
                            await DoMigration(sourceDatabaseName, result, onProgress, database, cancelToken);
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

        private async Task DoMigration(
            string databaseName, 
            SmugglerResult result, 
            Action<IOperationProgress> onProgress,
            DocumentDatabase database,
            OperationCancelToken cancelToken)
        {
            var state = GetLastMigrationState(database);
            const ItemType types = ItemType.Documents | ItemType.Indexes | ItemType.Transformers;
            //TODO: ItemType.Attachments;
            var databaseMigrationOptions = new DatabaseMigrationOptions
            {
                BatchSize = 1024,
                OperateOnTypes = types,
                //TODO: ExportDeletions = state != null,
                StartDocsEtag = state?.LastDocsEtag ?? LastEtagsInfo.EtagEmpty,
                StartDocsDeletionEtag = state?.LastDocDeleteEtag ?? LastEtagsInfo.EtagEmpty,
                StartAttachmentsEtag = state?.LastAttachmentsEtag ?? LastEtagsInfo.EtagEmpty,
                StartAttachmentsDeletionEtag = state?.LastAttachmentsDeleteEtag ?? LastEtagsInfo.EtagEmpty
            };

            var operationId = await GetOperationId(databaseName);
            var exportData = new ExportData
            {
                DownloadOptions = JsonConvert.SerializeObject(databaseMigrationOptions),
                ProgressTaskId = operationId
            };

            await MigrateDatabase(databaseName, exportData, database, result, onProgress, cancelToken);

            await SaveLastState(databaseName, operationId, database);
        }

        private async Task SaveLastState(string databaseName, long operationId, DocumentDatabase database)
        {
            var retries = 0;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                while (retries++ < 15)
                {
                    var operationStatus = await GetOperationStatus(databaseName, operationId, context);
                    if (operationStatus == null)
                        return;

                    if (operationStatus.TryGet("Completed", out bool completed) == false)
                        return;

                    if (completed == false)
                    {
                        await Task.Delay(1000, _cancellationToken);
                        continue;
                    }

                    if (operationStatus.TryGet("OperationState", out BlittableJsonReaderObject operationStateBlittable) == false)
                    {
                        // OperationState was added in the latest release of v3.5
                        return;
                    }

                    var operationState = JsonDeserializationServer.OperationState(operationStateBlittable);
                    operationStateBlittable = EntityToBlittable.ConvertEntityToBlittable(operationState, DocumentConventions.Default, context);
                    var cmd = new MergedPutCommand(operationStateBlittable, _migrationStateKey, null, database);
                    await database.TxMerger.Enqueue(cmd);
                    return;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationStatus(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var url = $"{_serverUrl}/databases/{databaseName}/operation/status?id={operationId}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, _cancellationToken);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the operation status was deleted before we could get it
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation status from server: {_serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            return await context.ReadForMemoryAsync(responseStream, "migration-operation-state");
        }

        private async Task MigrateDatabase(
            string databaseName, 
            ExportData exportData,
            DocumentDatabase database,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            OperationCancelToken cancelToken)
        {
            var url = $"{_serverUrl}/databases/{databaseName}/studio-tasks/exportDatabase";
            var json = JsonConvert.SerializeObject(exportData);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            };

            var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, _cancellationToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export database from server: {_serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var stream = new GZipStream(responseStream, CompressionMode.Decompress))
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var source = new StreamSource(stream, context);
                var destination = new DatabaseDestination(database);
                var options = new DatabaseSmugglerOptions();
                var smuggler = new DatabaseSmuggler(database, source, destination, database.Time, options, result, onProgress, cancelToken.Token);

                smuggler.Execute();
            }
        }

        private async Task<long> GetOperationId(string databaseName)
        {
            var url = $"{_serverUrl}/databases/{databaseName}/studio-tasks/next-operation-id";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, _cancellationToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation id from server: {_serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var bytes = await response.Content.ReadAsByteArrayAsync();
            var str = Encoding.UTF8.GetString(bytes, 0, bytes.Length);
            return long.Parse(str);
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

        private LastEtagsInfo GetLastMigrationState(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = database.DocumentsStorage.Get(context, _migrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.OperationState(document.Data);
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
