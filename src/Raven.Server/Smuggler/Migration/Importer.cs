using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using BackupUtils = Raven.Server.Utils.BackupUtils;

namespace Raven.Server.Smuggler.Migration
{
    public sealed class Importer : AbstractMigrator
    {
        private readonly int _buildVersion;

        public Importer(MigratorOptions options, MigratorParameters parameters, int buildVersion, AuthorizationStatus authorizationStatus) : base(options, parameters, authorizationStatus)
        {
            _buildVersion = buildVersion;
        }

        public override async Task Execute()
        {
            var importInfo = GetLastImportInfo();

            var operationId = await GetOperationId();

            await MigrateDatabase(operationId, importInfo);

            await SaveLastState(operationId, importInfo);

        }

        private async Task SaveLastState(long operationId, ImportInfo previousImportInfo)
        {
            var retries = 0;
            using (Parameters.Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                while (retries++ < 15)
                {
                    var operationState = await GetOperationState(Options.DatabaseName, operationId, context);
                    if (operationState == null)
                        return;

                    if (operationState.TryGet("Status", out OperationStatus operationStatus) == false)
                        return;

                    if (operationStatus == OperationStatus.InProgress)
                    {
                        await Task.Delay(1000, Parameters.CancelToken.Token);
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

                    if ((_buildVersion >= 40000 && _buildVersion < 41000) || _buildVersion == 40)
                    {
                        // prevent NRE, counter were added in 4.1
                        smugglerResult.Counters = new SmugglerProgressBase.CountsWithSkippedCountAndLastEtag();
                    }

                    if ((_buildVersion >= 40000 && _buildVersion < 50000) || (_buildVersion >= 40 && _buildVersion < 50))
                    {
                        // prevent NRE, time series were added in 5.0
                        smugglerResult.TimeSeries = new SmugglerProgressBase.CountsWithSkippedCountAndLastEtag();
                    }

                    if ((_buildVersion >= 4000 && _buildVersion <= 54133) || 
                        (_buildVersion >= 6000 && _buildVersion <= 60035) ||
                        (_buildVersion >= 40 && _buildVersion < 54))
                    {
                        // prevent NRE, time series deleted ranges were added in 5.4.201 and 6.0.105 
                        smugglerResult.TimeSeriesDeletedRanges = new SmugglerProgressBase.CountsWithSkippedCountAndLastEtag();
                    }

                    var importInfo = new ImportInfo
                    {
                        LastEtag = Math.Max(previousImportInfo?.LastEtag ?? 0, smugglerResult.GetLastEtag() + 1),
                        LastRaftIndex = Math.Max(previousImportInfo?.LastRaftIndex ?? 0, smugglerResult.GetLastRaftIndex() + 1),
                        ServerUrl = Options.ServerUrl,
                        DatabaseName = Options.DatabaseName
                    };

                    var importInfoBlittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(importInfo, context);
                    await SaveLastOperationState(importInfoBlittable);
                    return;
                }
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationState(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var url = $"{Options.ServerUrl}/databases/{databaseName}/operations/state?id={operationId}";
            var request = new HttpRequestMessage(HttpMethod.Get, url).WithConventions(DocumentConventions.DefaultForServer);
            var response = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the operation state was deleted before we could get it
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation state from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            return await context.ReadForMemoryAsync(responseStream, "migration-operation-state");
        }

        private async Task MigrateDatabase(long operationId, ImportInfo importInfo)
        {

            var startDocumentEtag = importInfo?.LastEtag ?? 0;
            var startRaftIndex = importInfo?.LastRaftIndex ?? 0;
            var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/smuggler/export?operationId={operationId}&startEtag={startDocumentEtag}&startRaftIndex={startRaftIndex}";
            var databaseSmugglerOptionsServerSide = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus)
            {
                OperateOnTypes = Options.OperateOnTypes,
                RemoveAnalyzers = Options.RemoveAnalyzers
            };

            if (importInfo != null)
            {
                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                {
                    databaseSmugglerOptionsServerSide.OperateOnTypes |= DatabaseItemType.Tombstones;
                    Options.OperateOnTypes |= DatabaseItemType.Tombstones;
                }

                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.CompareExchange))
                {
                    databaseSmugglerOptionsServerSide.OperateOnTypes |= DatabaseItemType.CompareExchangeTombstones;
                    Options.OperateOnTypes |= DatabaseItemType.CompareExchangeTombstones;
                }
            }

            var json = JsonConvert.SerializeObject(databaseSmugglerOptionsServerSide);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            }.WithConventions(DocumentConventions.DefaultForServer);

            var response = await Parameters.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Parameters.CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export database from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var options = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus)
            {
                TransformScript = Options.TransformScript,
                OperateOnTypes = Options.OperateOnTypes,
                OperateOnDatabaseRecordTypes = Options.OperateOnDatabaseRecordTypes
            };
            await using (var responseStream = await response.Content.ReadAsStreamAsync())
            await using (var stream = await BackupUtils.GetDecompressionStreamAsync(responseStream))
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(stream, context, Parameters.Database.Name, options))
            {
                var destination = Parameters.Database.Smuggler.CreateDestination();

                var smuggler = Parameters.Database.Smuggler.Create(source, destination, context, options, Parameters.Result, Parameters.OnProgress, Parameters.CancelToken.Token);

                await smuggler.ExecuteAsync();
            }
        }

        private async Task<long> GetOperationId()
        {
            var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/operations/next-operation-id";
            var request = new HttpRequestMessage(HttpMethod.Get, url).WithConventions(DocumentConventions.DefaultForServer);
            var response = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation id from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                var operationIdResponse = await context.ReadForMemoryAsync(responseStream, "operation-id");
                if (operationIdResponse.TryGet("Id", out long id) == false)
                {
                    throw new InvalidOperationException($"Failed to get operation id from server: {Options.ServerUrl}, " +
                                                        $"response: {operationIdResponse}");
                }

                return id;
            }
        }

        private ImportInfo GetLastImportInfo()
        {
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Parameters.Database.DocumentsStorage.Get(context, Options.MigrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.ImportInfo(document.Data);
            }
        }

        public static async Task<List<string>> GetDatabasesToMigrate(string serverUrl, RavenHttpClient httpClient, CancellationToken cancelToken)
        {
            var url = $"{serverUrl}/databases?namesOnly=true";
            var request = new HttpRequestMessage(HttpMethod.Get, url).WithConventions(DocumentConventions.DefaultForServer);
            var response = await httpClient.SendAsync(request, cancelToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get databases to migrate from server: {serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            await using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                var databaseList = await context.ReadForMemoryAsync(responseStream, "databases-list");

                if (databaseList.TryGet(nameof(DatabasesInfo.Databases), out BlittableJsonReaderArray names) == false)
                    throw new InvalidDataException($"Response is invalid, property {nameof(DatabasesInfo.Databases)} doesn't exist");

                return names.Select(x => x.ToString()).ToList();
            }
        }
    }

    public sealed class ImportInfo
    {
        public long LastEtag { get; set; }

        public long LastRaftIndex { get; set; }

        public string ServerUrl { get; set; }

        public string DatabaseName { get; set; }
    }
}
