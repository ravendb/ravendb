using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler
{
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

        public delegate Task ImportDelegate(JsonOperationContext context,
            Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            long operationId,
            OperationCancelToken token);

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

            var token = RequestHandler.CreateOperationToken();

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


                                    if (section.Headers.ContainsKey(Constants.Headers.ContentEncoding) && section.Headers[Constants.Headers.ContentEncoding] == "gzip")
                                    {
                                        await using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                        {
                                            blittableJson = await context.ReadForMemoryAsync(gzipStream, Constants.Smuggler.ImportOptions);
                                        }
                                    }
                                    else
                                    {
                                        blittableJson = await context.ReadForMemoryAsync(section.Body, Constants.Smuggler.ImportOptions);
                                    }

                                    options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                                    continue;
                                }

                                if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition) == false)
                                    continue;

                                ApplyBackwardCompatibility(options);
                                var inputStream = GetInputStream(section.Body, options);
                                var stream = new GZipStream(inputStream, CompressionMode.Decompress);
                                await onImport(context, stream, options, result, onProgress, operationId, token);

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
        }
    }
}
