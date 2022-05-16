using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        public delegate Task<IOperationResult> ExportDelegate(DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext context,
            OperationCancelToken token);

        internal async Task Export(JsonOperationContext context, string databaseName, ExportDelegate onExport,
            Documents.Operations.Operations operations, long operationId, DocumentDatabase documentDatabase = null)
        {
            var startDocumentEtag = GetLongQueryString("startEtag", false) ?? 0;
            var startRaftIndex = GetLongQueryString("startRaftIndex", false) ?? 0;
            var stream = TryGetRequestFromStream("DownloadOptions") ?? RequestBodyStream();
            DatabaseSmugglerOptionsServerSide options;
            using (context.GetMemoryBuffer(out var buffer))
            {
                var firstRead = await stream.ReadAsync(buffer.Memory.Memory);
                buffer.Used = 0;
                buffer.Valid = firstRead;
                if (firstRead != 0)
                {
                    var blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                    options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                }
                else
                {
                    // no content, we'll use defaults
                    options = new DatabaseSmugglerOptionsServerSide();
                }
            }

            if (string.IsNullOrWhiteSpace(options.EncryptionKey) == false)
                ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            if (feature == null)
                options.AuthorizationStatus = AuthorizationStatus.DatabaseAdmin;
            else
                options.AuthorizationStatus = feature.CanAccess(databaseName, requireAdmin: true, requireWrite: false)
                    ? AuthorizationStatus.DatabaseAdmin
                    : AuthorizationStatus.ValidUser;

            var fileName = options.FileName;
            if (string.IsNullOrEmpty(fileName))
            {
                fileName = $"Dump of {databaseName} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
            }

            var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.Headers["Content-Type"] = "application/octet-stream";
            ApplyBackwardCompatibility(options);
            var token = CreateOperationToken();

            await operations.AddOperation(
                documentDatabase.Name,
                "Export database: " + databaseName,
                OperationType.DatabaseExport,
                onProgress => onExport(options, startDocumentEtag, startRaftIndex, onProgress, context, token), operationId, token: token);

        }

        internal void ApplyBackwardCompatibility(DatabaseSmugglerOptionsServerSide options)
        {
            if (options == null)
                return;

            if (((options.OperateOnTypes & DatabaseItemType.DatabaseRecord) != 0)
                && (options.OperateOnDatabaseRecordTypes == DatabaseRecordItemType.None))
            {
                options.OperateOnDatabaseRecordTypes = DatabaseSmugglerOptions.DefaultOperateOnDatabaseRecordTypes;
            }

            if (RequestRouter.TryGetClientVersion(HttpContext, out var version) == false)
                return;

            if (version.Major != RavenVersionAttribute.Instance.MajorVersion)
                return;

#pragma warning disable 618
            if (version.Minor < 2 && options.OperateOnTypes.HasFlag(DatabaseItemType.Counters))
#pragma warning restore 618
            {
                options.OperateOnTypes |= DatabaseItemType.CounterGroups;
            }

            // only all 4.0 and 4.1 less or equal to 41006
            if (version.Revision < 70 || version.Revision > 41006)
                return;

            if (options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                options.OperateOnTypes |= DatabaseItemType.Attachments;
        }

        internal async ValueTask WriteResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }

        internal Stream GetOutputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey == null)
                return fileStream;

            var key = options?.EncryptionKey;
            return new EncryptingXChaCha20Poly1305Stream(fileStream,
                Convert.FromBase64String(key));
        }

        internal static Stream GetInputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey != null)
                return new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(options.EncryptionKey));

            return fileStream;
        }

        public delegate Task ImportDelegate(JsonOperationContext context,
            Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            OperationCancelToken token);

        internal async Task Import(JsonOperationContext context, string databaseName, ImportDelegate onImport,
            Documents.Operations.Operations operations, long operationId, DocumentDatabase documentDatabase = null)
        {
            if (HttpContext.Request.HasFormContentType == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad request
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Error"] = "This endpoint requires form content type" });
                    return;
                }
            }

            var token = CreateOperationToken();

            var result = new SmugglerResult();
            BlittableJsonReaderObject blittableJson = null;
            await operations.AddOperation(documentDatabase.Name
                , "Import to: " + databaseName,
                OperationType.DatabaseImport,
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


                                    if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
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
                                await onImport(context, stream, options, result, onProgress, token);

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
                }, operationId, token: token).ConfigureAwait(false);

            await WriteImportResultAsync(context, result, ResponseBodyStream());
        }

        internal async ValueTask WriteImportResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }

        internal async Task InternalGetStateAsync(OperationState state, JsonOperationContext context)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, state.ToJson());
                // writes Patch response
                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Operations);
            }
        }

        internal void TrafficWatchQuery(IndexQueryServerSide indexQuery)
        {
            var sb = new StringBuilder();

            // append stringBuilder with the query
            sb.Append(indexQuery.Query);
            // if query got parameters append with parameters
            if (indexQuery.QueryParameters != null && indexQuery.QueryParameters.Count > 0)
                sb.AppendLine().Append(indexQuery.QueryParameters);

            AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.Queries);
        }
    }
}
