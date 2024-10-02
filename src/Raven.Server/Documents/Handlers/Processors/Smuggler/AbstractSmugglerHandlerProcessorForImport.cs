using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Properties;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using RavenServerBackupUtils = Raven.Server.Utils.BackupUtils;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
    public delegate Task<SmugglerResult> ImportDelegate(JsonOperationContext context,
        Stream stream,
        DatabaseSmugglerOptionsServerSide options,
        SmugglerResult result,
        Action<IOperationProgress> onProgress,
        long operationId,
        OperationCancelToken token);

    internal abstract class AbstractSmugglerHandlerProcessorForImport<TRequestHandler, TOperationContext> : AbstractSmugglerHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractSmugglerHandlerProcessorForImport([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask ImportAsync(JsonOperationContext context, long? operationId);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = RequestHandler.GetLongQueryString("operationId", required: false);
                await ImportAsync(context, operationId);
            }
        }

        internal async Task Import<TOperation>(JsonOperationContext context, string databaseName, ImportDelegate onImport,
            AbstractOperations<TOperation> operations, long operationId)
            where TOperation : AbstractOperation, new()
        {
            if (HttpContext.Request.HasFormContentType == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad request
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Error"] = "This endpoint requires form content type" });
                    return;
                }
            }

            var token = RequestHandler.CreateHttpRequestBoundOperationToken();

            var result = new SmugglerResult();
            BlittableJsonReaderObject blittableJson = null;

            await operations.AddLocalOperation(
                operationId,
                OperationType.DatabaseImport,
                "Import to: " + databaseName,
                detailedDescription: null,
                onProgress =>
                {
                    return Task.Run(async () =>
                    {
                        try
                        {
                            var boundary = MultipartRequestHelper.GetBoundary(
                                MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                                MultipartRequestHelper.MultipartBoundaryLengthLimit);
                            var reader = new MultipartReader(boundary, HttpContext.Request.Body);
                            DatabaseSmugglerOptionsServerSide options = null;

                            while (true)
                            {
                                var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                                if (section == null)
                                    break;

                                if (ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out ContentDispositionHeaderValue contentDisposition) == false)
                                    continue;

                                if (MultipartRequestHelper.HasFormDataContentDisposition(contentDisposition))
                                {
                                    var key = HeaderUtilities.RemoveQuotes(contentDisposition.Name);
                                    if (key != Constants.Smuggler.ImportOptions)
                                        continue;

                                    await using (var stream = RequestHandler.GetDecompressedStream(section.Body, section.Headers))
                                        blittableJson = await context.ReadForMemoryAsync(stream, Constants.Smuggler.ImportOptions);

                                    IgnoreDatabaseItemTypesIfCurrentVersionIsOlderThenClientVersion(context, ref blittableJson);

                                    options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                                    options.SetAuthorizationStatus(RequestHandler.GetAuthorizationStatusForSmuggler(RequestHandler.DatabaseName));

                                    continue;
                                }

                                if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition) == false)
                                    continue;

                                ApplyBackwardCompatibility(options);
                                await using (var inputStream = await GetInputStreamAsync(section.Body, options))
                                await using (var stream = await RavenServerBackupUtils.GetDecompressionStreamAsync(inputStream))
                                {
                                    await onImport(context, stream, options, result, onProgress, operationId, token);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            result.AddError($"Error occurred during import. Exception: {e.Message}");
                            onProgress.Invoke(result.Progress);
                            throw;
                        }

                        return (IOperationResult)result;
                    });
                },
                token: token).ConfigureAwait(false);

            await WriteSmugglerResultAsync(context, result, RequestHandler.ResponseBodyStream());

            RequestHandler.LogTaskToAudit(OperationType.DatabaseImport.ToString(), operationId, configuration: null);
        }

        private void IgnoreDatabaseItemTypesIfCurrentVersionIsOlderThenClientVersion(JsonOperationContext context, ref BlittableJsonReaderObject blittableJson)
        {
            DynamicJsonValue djv = null;

            if (blittableJson.TryGet(nameof(DatabaseSmugglerOptions.OperateOnTypes), out string operateOnTypes) &&
                operateOnTypes != null && 
                Enum.TryParse(typeof(DatabaseItemType), operateOnTypes, out _) == false)
            {
                CheckClientVersion(operateOnTypes, nameof(DatabaseSmugglerOptions.OperateOnTypes));

                var itemsTypes = DatabaseItemType.None;
                var types = operateOnTypes.Split(", ");

                foreach (var type in types)
                {
                    if (Enum.TryParse(typeof(DatabaseItemType), type, true, out var result) == false || result is DatabaseItemType existing == false)
                        continue;

                    itemsTypes |= existing;
                }

                djv = new DynamicJsonValue(blittableJson)
                {
                    [nameof(DatabaseSmugglerOptions.OperateOnTypes)] = itemsTypes
                };
            }

            if (blittableJson.TryGet(nameof(DatabaseSmugglerOptions.OperateOnDatabaseRecordTypes), out string operateOnDatabaseRecordTypes) &&
                operateOnDatabaseRecordTypes != null && 
                Enum.TryParse(typeof(DatabaseRecordItemType), operateOnDatabaseRecordTypes, out _) == false)
            {
                CheckClientVersion(operateOnDatabaseRecordTypes, nameof(DatabaseSmugglerOptions.OperateOnDatabaseRecordTypes));

                var databaseRecordItemsTypes = DatabaseRecordItemType.None;
                var types = operateOnDatabaseRecordTypes.Split(", ");

                foreach (var type in types)
                {
                    if (Enum.TryParse(typeof(DatabaseRecordItemType), type, true, out var result) == false || result is DatabaseRecordItemType existing == false)
                        continue;

                    databaseRecordItemsTypes |= existing;
                }

                if (djv == null)
                    djv = new DynamicJsonValue(blittableJson);

                djv[nameof(DatabaseSmugglerOptions.OperateOnDatabaseRecordTypes)] = databaseRecordItemsTypes;
            }

            if (djv == null)
                return;

            blittableJson.Modifications = djv;
            using (var old = blittableJson)
            {
                blittableJson = context.ReadObject(blittableJson, "item-types");
            }
        }

        private void CheckClientVersion(string value, string propertyName)
        {
            if (RequestRouter.TryGetClientVersion(HttpContext, out var version) == false ||
                (Version.TryParse(RavenVersionAttribute.Instance.AssemblyVersion, out var existingVersion) && version.CompareTo(existingVersion) <= 0))
                throw new InvalidDataException($"The value '{value}' supplied in '{propertyName}' is not parsable");
        }
    }
}
