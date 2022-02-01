using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideDebugInfoPackageHandler : RequestHandler
    {
        internal const string _serverWidePrefix = "server-wide";
        internal static readonly string[] FieldsThatShouldBeExposedForDebug = new string[]
        {
            nameof(DatabaseRecord.DatabaseName),
            nameof(DatabaseRecord.Encrypted),
            nameof(DatabaseRecord.Disabled),
            nameof(DatabaseRecord.EtagForBackup),
            nameof(DatabaseRecord.DeletionInProgress),
            nameof(DatabaseRecord.DatabaseState),
            nameof(DatabaseRecord.Topology),
            nameof(DatabaseRecord.ConflictSolverConfig),
            nameof(DatabaseRecord.Sorters),
            nameof(DatabaseRecord.Indexes),
            nameof(DatabaseRecord.IndexesHistory),
            nameof(DatabaseRecord.AutoIndexes),
            nameof(DatabaseRecord.Revisions),
            nameof(DatabaseRecord.RevisionsForConflicts),
            nameof(DatabaseRecord.Expiration),
            nameof(DatabaseRecord.Refresh),
            nameof(DatabaseRecord.Client),
            nameof(DatabaseRecord.Studio),
            nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount),
            nameof(DatabaseRecord.UnusedDatabaseIds),
        };

        [RavenAction("/admin/debug/remote-cluster-info-package", "GET", AuthorizationStatus.Operator)]
        public async Task GetClusterWideInfoPackageForRemote()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        NodeDebugInfoRequestHeader requestHeader;
                        using (var requestHeaderJson =
                            await transactionOperationContext.ReadForMemoryAsync(HttpContext.Request.Body, "remote-cluster-info-package/read request header"))
                        {
                            requestHeader = JsonDeserializationServer.NodeDebugInfoRequestHeader(requestHeaderJson);
                        }

                        await WriteServerInfo(archive, jsonOperationContext, localEndpointClient);
                        foreach (var databaseName in requestHeader.DatabaseNames)
                        {
                            await WriteDatabaseRecord(archive, databaseName, jsonOperationContext, transactionOperationContext);
                            await WriteDatabaseInfo(archive, jsonOperationContext, localEndpointClient, databaseName);
                        }

                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        [RavenAction("/admin/debug/cluster-info-package", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetClusterWideInfoPackage()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} Cluster Wide.zip";

            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;

            var token = CreateOperationToken();
            var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();

            await ServerStore.Operations.AddOperation(null, "Created debug package for all cluster nodes", Operations.Operations.OperationType.DebugPackage, async _ =>
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
                using (transactionOperationContext.OpenReadTransaction())
                {
                    await using (var ms = new MemoryStream())
                    {
                        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                        {
                            var topology = ServerStore.GetClusterTopology(transactionOperationContext);
                            
                            foreach (var (tag, url) in topology.AllNodes)
                            {
                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(jsonOperationContext, archive, tag, url, Server.Certificate.Certificate);
                                }
                                catch (Exception e)
                                {
                                    DebugInfoPackageUtils.WriteExceptionAsZipEntry(e, archive, $"Node - [{tag}]");
                                }
                            }
                        }

                        ms.Position = 0;
                        await ms.CopyToAsync(ResponseBodyStream(), token.Token);
                    }
                }

                return null;
            }, operationId, token: token);
        }

        private async Task WriteDebugInfoPackageForNodeAsync(
            JsonOperationContext context,
            ZipArchive archive,
            string tag,
            string url,
            X509Certificate2 certificate)
        {
            //note : theoretically GetDebugInfoFromNodeAsync() can throw, error handling is done at the level of WriteDebugInfoPackageForNodeAsync() calls
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, certificate))
            {
                var timeout = TimeSpan.FromMinutes(1);
                if (ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan > timeout)
                    timeout = ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan;

                requestExecutor.DefaultTimeout = timeout;

                using (var responseStream = await GetDebugInfoFromNodeAsync(
                    context,
                    requestExecutor))
                {
                    var entry = archive.CreateEntry($"Node - [{tag}].zip");
                    entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                    using (var entryStream = entry.Open())
                    {
                        await responseStream.CopyToAsync(entryStream);
                        await entryStream.FlushAsync();
                    }
                }
            }
        }

        [RavenAction("/admin/debug/info-package", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Node [{ServerStore.NodeTag}].zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;

            var token = CreateOperationToken();

            var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();

            await ServerStore.Operations.AddOperation(null, "Created debug package for current server only", Operations.Operations.OperationType.DebugPackage, async _ =>
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        
                        await WriteServerInfo(archive, context, localEndpointClient, token.Token);
                        await WriteForAllLocalDatabases(archive, context, localEndpointClient, token: token.Token);
                        await WriteLogFile(archive, token.Token);
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream(), token.Token);
                }

                return null;
            }, operationId, token: token);
        }

        private static async Task WriteLogFile(ZipArchive archive, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            var prefix = $"{_serverWidePrefix}/{DateTime.UtcNow:yyyy-MM-dd H:mm:ss}.txt";

            try
            {
                var entry = archive.CreateEntry(prefix, CompressionLevel.Optimal);
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                using (var entryStream = entry.Open())
                {
                    LoggingSource.Instance.AttachPipeSink(entryStream);

                    await Task.Delay(15000, token);
                    LoggingSource.Instance.DetachPipeSink();

                    await entryStream.FlushAsync(token);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                LoggingSource.Instance.DetachPipeSink();
                DebugInfoPackageUtils.WriteExceptionAsZipEntry(e, archive, prefix);
            }
        }

        private async Task<Stream> GetDebugInfoFromNodeAsync(
            JsonOperationContext context,
            RequestExecutor requestExecutor)
        {
            var rawStreamCommand = new GetRawStreamResultCommand($"/admin/debug/info-package");
            await requestExecutor.ExecuteAsync(rawStreamCommand, context);
            rawStreamCommand.Result.Position = 0;
            return rawStreamCommand.Result;
        }

        private async Task WriteForAllLocalDatabases(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                foreach (var databaseName in ServerStore.Cluster.GetDatabaseNames(transactionOperationContext))
                {
                    token.ThrowIfCancellationRequested();

                    using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(transactionOperationContext, databaseName))
                    {
                        if (rawRecord == null ||
                            rawRecord.Topology.RelevantFor(ServerStore.NodeTag) == false)
                            continue;
                        
                        await WriteDatabaseRecord(archive, databaseName, jsonOperationContext, transactionOperationContext, token);

                        if (rawRecord.IsDisabled ||
                            rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress ||
                            IsDatabaseBeingDeleted(ServerStore.NodeTag, rawRecord))
                            continue;

                        await WriteDatabaseInfo(archive, jsonOperationContext, localEndpointClient, databaseName, token);
                    }
                }
            }
        }

        private static bool IsDatabaseBeingDeleted(string tag, RawDatabaseRecord databaseRecord)
        {
            if (databaseRecord == null)
                return false;

            var deletionInProgress = databaseRecord.DeletionInProgress;

            return deletionInProgress != null && deletionInProgress.TryGetValue(tag, out var delInProgress) && delInProgress != DeletionInProgressStatus.No;
        }
        
        private async Task WriteDatabaseInfo(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient,
            string databaseName, CancellationToken token = default)
        {
            var endpointParameters = new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>()
            {
                { "database", new Microsoft.Extensions.Primitives.StringValues(databaseName) }
            };
            await WriteForServerOrDatabase(archive, jsonOperationContext, localEndpointClient, RouteInformation.RouteType.Databases, databaseName, databaseName, endpointParameters, token);
        }

        private async Task WriteServerInfo(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, CancellationToken token = default)
        {
            await WriteForServerOrDatabase(archive, jsonOperationContext, localEndpointClient, RouteInformation.RouteType.None, _serverWidePrefix, null, null, token);
        }

        private async Task WriteForServerOrDatabase(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient,
            RouteInformation.RouteType routeType, string path, string databaseName,
            Dictionary<string, Microsoft.Extensions.Primitives.StringValues> endpointParameters = null,
            CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            var routes = DebugInfoPackageUtils.GetAuthorizedRoutes(Server, HttpContext, databaseName).Where(x => x.TypeOfRoute == routeType);

            foreach (var route in routes)
            {
                token.ThrowIfCancellationRequested();
                
                await InvokeAndWriteToArchive(archive, jsonOperationContext, localEndpointClient, route, path, endpointParameters, token);
            }
        }

        internal static async Task InvokeAndWriteToArchive(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient,
            RouteInformation route, string path,
            Dictionary<string, Microsoft.Extensions.Primitives.StringValues> endpointParameters = null,
            CancellationToken token = default)
        {
            try
            {
                var response = await localEndpointClient.InvokeAsync(route, endpointParameters);

                var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path, response.ContentType == "text/plain" ? "txt" : "json");
                var entry = archive.CreateEntry(entryName);
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                using (var entryStream = entry.Open())
                {
                    if (response.ContentType == "text/plain")
                    {
                        await response.Body.CopyToAsync(entryStream, token);
                    }
                    else
                    {
                        using (var writer = new BlittableJsonTextWriter(jsonOperationContext, entryStream))
                        {
                            var endpointOutput = jsonOperationContext.ReadForMemory(response.Body, $"read/local endpoint/{route.Path}");
                            jsonOperationContext.Write(writer, endpointOutput);
                            writer.Flush();
                        }
                    }
                    await entryStream.FlushAsync(token);
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                //precaution, ideally this exception should never be thrown
                if (e is InvalidStartOfObjectException)
                    e = new InvalidOperationException("Expected to find a blittable object as a result of debug endpoint, but found something else (see inner exception for details). This should be investigated as all RavenDB endpoints are supposed to return an object.", e);

                DebugInfoPackageUtils.WriteExceptionAsZipEntry(e, archive, DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path, null));
            }
        }

        private async Task WriteDatabaseRecord(ZipArchive archive, string databaseName, JsonOperationContext jsonOperationContext, TransactionOperationContext transactionCtx, CancellationToken token = default)
        {
            var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation("/database-record", databaseName, "json");
            try
            {
                var entry = archive.CreateEntry(entryName);
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                using (var entryStream = entry.Open())
                using (var writer = new BlittableJsonTextWriter(jsonOperationContext, entryStream))
                {
                    jsonOperationContext.Write(writer, GetDatabaseRecordForDebugPackage(transactionCtx, databaseName));
                    writer.Flush();
                    await entryStream.FlushAsync(token);
                }
            }
            catch (Exception e)
            {
                DebugInfoPackageUtils.WriteExceptionAsZipEntry(e, archive, entryName);
            }
        }
        
        public BlittableJsonReaderObject GetDatabaseRecordForDebugPackage(TransactionOperationContext context, string databaseName)
        {
            var databaseRecord = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName);

            if (databaseRecord == null)
                throw new RavenException($"Couldn't fetch {nameof(DatabaseRecord)} from server for database '{databaseName}'");

            var djv = new DynamicJsonValue();
            foreach (string fld in FieldsThatShouldBeExposedForDebug)
            {
                if (databaseRecord.Raw.TryGetMember(fld, out var obj))
                {
                    djv[fld] = obj;
                }
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonContext))
            {
                return jsonContext.ReadObject(djv, "databaserecord");
            }
        }

        internal class NodeDebugInfoRequestHeader
        {
            public string FromUrl { get; set; }

            public List<string> DatabaseNames { get; set; }
        }
    }
}
