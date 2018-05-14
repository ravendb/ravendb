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
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public class Importer : AbstractMigrator
    {
        public Importer(MigratorOptions options) : base(options)
        {
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
                        LastEtag = smugglerResult.GetLastEtag() + 1,
                        ServerUrl = ServerUrl,
                        DatabaseName = DatabaseName
                    };

                    var importInfoBlittable = EntityToBlittable.ConvertCommandToBlittable(importInfo, context);
                    await SaveLastOperationState(importInfoBlittable);
                    return;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationState(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var url = $"{ServerUrl}/databases/{databaseName}/operations/state?id={operationId}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await HttpClient.SendAsync(request, CancelToken.Token);
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
            var databaseSmugglerOptionsServerSide = new DatabaseSmugglerOptionsServerSide
            {
                OperateOnTypes = OperateOnTypes,
                RemoveAnalyzers = RemoveAnalyzers
            };

            if (importInfo != null)
                databaseSmugglerOptionsServerSide.OperateOnTypes |= DatabaseItemType.Tombstones;

            var json = JsonConvert.SerializeObject(databaseSmugglerOptionsServerSide);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            };

            var response = await HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, CancelToken.Token);
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
            using (var source = new StreamSource(stream, context, Database))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide();
                var smuggler = new Documents.DatabaseSmuggler(Database, source, destination, Database.Time, options, Result, OnProgress, CancelToken.Token);

                smuggler.Execute();
            }
        }

        private async Task<long> GetOperationId()
        {
            var url = $"{ServerUrl}/databases/{DatabaseName}/operations/next-operation-id";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await HttpClient.SendAsync(request, CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation id from server: {ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                var operationIdResponse = await context.ReadForMemoryAsync(responseStream, "operation-id");
                if (operationIdResponse.TryGet("Id", out long id) == false)
                {
                    throw new InvalidOperationException($"Failed to get operation id from server: {ServerUrl}, " +
                                                        $"response: {operationIdResponse}");
                }

                return id;
            }
        }

        private ImportInfo GetLastImportInfo()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, MigrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.ImportInfo(document.Data);
            }
        }

        public static async Task<List<string>> GetDatabasesToMigrate(string serverUrl, HttpClient httpClient, CancellationToken cancelToken)
        {
            var url = $"{serverUrl}/databases?namesOnly=true";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await httpClient.SendAsync(request, cancelToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get databases to migrate from server: {serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                var databaseList = await context.ReadForMemoryAsync(responseStream, "databases-list");

                if (databaseList.TryGet(nameof(DatabasesInfo.Databases), out BlittableJsonReaderArray names) == false)
                    throw new InvalidDataException($"Response is invalid, property {nameof(DatabasesInfo.Databases)} doesn't exist");

                return names.Select(x => x.ToString()).ToList();
            }
        }
    }

    public class ImportInfo
    {
        public long LastEtag { get; set; }

        public string ServerUrl { get; set; }

        public string DatabaseName { get; set; }
    }
}
