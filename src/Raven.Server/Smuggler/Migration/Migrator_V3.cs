using System;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator_V3: AbstractMigrator
    {
        private readonly HttpClient _client;
        private readonly string _migrationStateKey;
        private readonly MajorVersion _majorVersion;

        public Migrator_V3(
            string serverUrl,
            string databaseName,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            DocumentDatabase database,
            HttpClient client,
            string migrationStateKey,
            MajorVersion marjorVersion,
            OperationCancelToken cancelToken)
            : base(serverUrl, databaseName, result, onProgress, database, cancelToken)
        {
            _client = client;
            _migrationStateKey = migrationStateKey;
            _majorVersion = marjorVersion;
        }

        public override async Task Execute()
        {
            var state = GetLastMigrationState();
            const ItemType types = ItemType.Documents | ItemType.Indexes | ItemType.Transformers | ItemType.Attachments;

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

            // getting a new operation id was added in v3.5
            var operationId = _majorVersion == MajorVersion.V30 ? 0 : await GetOperationId();

            object exportData;
            if (_majorVersion == MajorVersion.V30)
            {
                exportData = new ExportDataV3
                {
                    SmugglerOptions = JsonConvert.SerializeObject(databaseMigrationOptions)
                };
            }
            else
            {
                exportData = new ExportDataV35
                {
                    DownloadOptions = JsonConvert.SerializeObject(databaseMigrationOptions),
                    ProgressTaskId = operationId
                };
            }

            var exportOptions = JsonConvert.SerializeObject(exportData);
            await MigrateDatabase(exportOptions);

            await SaveLastState(operationId);
        }

        private async Task MigrateDatabase(string json)
        {
            var url = $"{ServerUrl}/databases/{DatabaseName}/studio-tasks/exportDatabase";
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            };

            var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export database from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var stream = new GZipStream(responseStream, mode: CompressionMode.Decompress))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(stream, context))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide();
                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, Result, OnProgress, CancelToken.Token);

                smuggler.Execute();
            }
        }

        private async Task<long> GetOperationId()
        {
            var url = $"{ServerUrl}/databases/{DatabaseName}/studio-tasks/next-operation-id";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation id from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var bytes = await response.Content.ReadAsByteArrayAsync();
            var str = Encoding.UTF8.GetString(bytes, 0, bytes.Length);
            return long.Parse(str);
        }

        private async Task SaveLastState(long operationId)
        {
            var retries = 0;
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                while (retries++ < 15)
                {
                    var operationStatus = await GetOperationStatus(DatabaseName, operationId, context);
                    if (operationStatus == null)
                        return;

                    if (operationStatus.TryGet("Completed", out bool completed) == false)
                        return;

                    if (completed == false)
                    {
                        await Task.Delay(1000, CancelToken.Token);
                        continue;
                    }

                    if (operationStatus.TryGet("OperationState", out BlittableJsonReaderObject operationStateBlittable) == false)
                    {
                        // OperationState was added in the latest release of v3.5
                        return;
                    }

                    var blittableCopy = context.ReadObject(operationStateBlittable, _migrationStateKey);
                    var cmd = new MergedPutCommand(blittableCopy, _migrationStateKey, null, Database);
                    await Database.TxMerger.Enqueue(cmd);
                    return;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationStatus(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var url = $"{ServerUrl}/databases/{databaseName}/operation/status?id={operationId}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, CancelToken.Token);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the operation status was deleted before we could get it
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation status from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            return await context.ReadForMemoryAsync(responseStream, "migration-operation-state");
        }

        private LastEtagsInfo GetLastMigrationState()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, _migrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.OperationState(document.Data);
            }
        }

        public override void Dispose()
        {
            _client.Dispose();
        }
    }
}
