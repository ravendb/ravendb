using System;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public class Importer : AbstractMigrator
    {
        private readonly string _migrationStateKey;
        private readonly RequestExecutor _requestExecutor;
        private readonly HttpClient _client;

        public Importer(
            string serverUrl, 
            string sourceDatabaseName, 
            SmugglerResult result, 
            Action<IOperationProgress> onProgress, 
            DocumentDatabase database, 
            string migrationStateKey, 
            OperationCancelToken cancelToken) 
            : base(serverUrl, sourceDatabaseName, result, onProgress, database, cancelToken)
        {
            _migrationStateKey = migrationStateKey;

            _requestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(ServerUrl, DatabaseName, Database.ServerStore.Server.ClusterCertificateHolder.Certificate, DocumentConventions.Default);

            _client = _requestExecutor.HttpClient;
        }

        public override async Task Execute()
        {
            var importInfo = GetLastImportInfo();

            var operationId = await GetOperationId();

            await MigrateDatabase(operationId, importInfo);

            await SaveLastState(operationId);
        }

        private async Task SaveLastState(long operationId)
        {
            var retries = 0;
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                while (retries++ < 15)
                {
                    var operationState = await GetOperationState(DatabaseName, operationId, context);
                    if (operationState == null)
                        return;

                    if (operationState.TryGet("Status", out OperationStatus operationStatus) == false)
                        return;

                    if (operationStatus == OperationStatus.InProgress)
                    {
                        await Task.Delay(1000, CancelToken.Token);
                        continue;
                    }

                    if (operationStatus == OperationStatus.Canceled || operationStatus == OperationStatus.Faulted)
                    {
                        throw new InvalidOperationException("Couldn't get last operation state because the " +
                                                            $"operation state is {operationStatus.ToString()} " +
                                                            "although the operation was completed successfully");
                    }

                    if (operationState.TryGet("Result", out BlittableJsonReaderObject smugglerResultBlittable) == false)
                        return;

                    var smugglerResult = JsonDeserializationClient.SmugglerResult(smugglerResultBlittable);
                    if (smugglerResult == null)
                        return;

                    var importInfo = new ImportInfo
                    {
                        LastEtag = smugglerResult.GetLastEtag()
                    };
                
                    var importInfoBlittable = EntityToBlittable.ConvertEntityToBlittable(importInfo, DocumentConventions.Default, context);
                    var cmd = new MergedPutCommand(importInfoBlittable, _migrationStateKey, null, Database);
                    await Database.TxMerger.Enqueue(cmd);
                    return;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationState(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var url = $"{ServerUrl}/databases/{databaseName}/operations/state?id={operationId}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await _client.SendAsync(request, CancelToken.Token);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the operation state was deleted before we could get it
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation state from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            return await context.ReadForMemoryAsync(responseStream, "migration-operation-state");
        }

        private async Task MigrateDatabase(long operationId, ImportInfo importInfo)
        {
            var startDocumentEtag = importInfo?.LastEtag ?? 0;
            var url = $"{ServerUrl}/databases/{DatabaseName}/smuggler/export?operationId={operationId}&startEtag={startDocumentEtag}";
            var json = JsonConvert.SerializeObject(new DatabaseSmugglerOptionsServerSide());
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
                var smuggler = new Documents.DatabaseSmuggler(Database, source, destination, Database.Time, options, Result, OnProgress, CancelToken.Token);

                smuggler.Execute();
            }
        }

        private async Task<long> GetOperationId()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var getNextOperationIdRequest = new GetNextOperationIdCommand();
                await _requestExecutor.ExecuteAsync(getNextOperationIdRequest, context, CancelToken.Token);
                return getNextOperationIdRequest.Result;
            }
        }

        private ImportInfo GetLastImportInfo()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, _migrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.ImportInfo(document.Data);
            }
        }

        public override void Dispose()
        {
            _requestExecutor.Dispose();
        }
    }

    public class ImportInfo
    {
        public long LastEtag { get; set; }
    }
}
