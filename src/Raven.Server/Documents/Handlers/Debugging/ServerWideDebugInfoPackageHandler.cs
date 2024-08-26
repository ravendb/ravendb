using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Logging;
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
    public sealed class ServerWideDebugInfoPackageHandler : ServerRequestHandler
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
            nameof(DatabaseRecord.DataArchival),
            nameof(DatabaseRecord.Client),
            nameof(DatabaseRecord.Studio),
            nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount),
            nameof(DatabaseRecord.UnusedDatabaseIds),
            nameof(DatabaseRecord.RollingIndexes),
            nameof(DatabaseRecord.LockMode),
            nameof(DatabaseRecord.DocumentsCompression),
            nameof(DatabaseRecord.Analyzers),
            nameof(DatabaseRecord.TimeSeries),
            nameof(DatabaseRecord.SupportedFeatures),

            nameof(DatabaseRecord.Sharding)
        };

        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<ServerWideDebugInfoPackageHandler>();

        [RavenAction("/admin/debug/remote-cluster-info-package", "GET", AuthorizationStatus.Operator)]
        public async Task GetClusterWideInfoPackageForRemote()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                await using (var ms = new MemoryStream())
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
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H-mm-ss} Cluster Wide.zip";
            HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = contentDisposition;
            HttpContext.Response.Headers[Constants.Headers.ContentType] = "application/zip";

            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
                topology = ServerStore.GetClusterTopology(ctx);

            var timeoutInSecPerNode = GetIntValueQueryString("timeoutInSecPerNode", false) ?? 60 * 60;
            var clusterOperationToken = CreateHttpRequestBoundOperationToken();
            var type = GetDebugInfoPackageContentType();
            var databases = GetStringValuesQueryString("database", required: false);
            var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();

            await ServerStore.Operations.AddLocalOperation(
                operationId,
                OperationType.DebugPackage,
                "Created debug package for all cluster nodes",
                detailedDescription: null,
                async _ =>
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        foreach (var (tag, url) in topology.AllNodes)
                        {
                            try
                            {
                                await WriteDebugInfoPackageForNodeAsync(jsonOperationContext, archive, tag, url, clusterOperationToken, timeoutInSecPerNode, databases, type);
                            }
                            catch (Exception e)
                            {
                                await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, $"Node - [{ServerStore.NodeTag}]");
                            }
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }

                return null;
            }, token: clusterOperationToken);
        }

        private async Task WriteDebugInfoPackageForNodeAsync(JsonOperationContext context, ZipArchive archive, string tag, string url, OperationCancelToken clusterOperationToken, int timeoutInSecPerNode, StringValues databases, DebugInfoPackageContentType contentType)
        {
            //note : theoretically GetDebugInfoFromNodeAsync() can throw, error handling is done at the level of WriteDebugInfoPackageForNodeAsync() calls
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(url, Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                var nextOperationId = new GetNextServerOperationIdCommand();
                await requestExecutor.ExecuteAsync(nextOperationId, context);

                await using (var responseStream = await GetDebugInfoFromNodeAsync(context, requestExecutor, nextOperationId.Result, clusterOperationToken, timeoutInSecPerNode, databases, contentType))
                {
                    var entry = archive.CreateEntry($"Node - [{tag}].zip");
                    entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                    await using (var entryStream = entry.Open())
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
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H-mm-ss} - Node [{ServerStore.NodeTag}].zip";
            HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = contentDisposition;
            HttpContext.Response.Headers[Constants.Headers.ContentType] = "application/zip";

            var debugInfoType = GetDebugInfoPackageContentType();
            var databases = GetStringValuesQueryString("database", required: false);
            var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();
            var token = CreateHttpRequestBoundOperationToken();

            await ServerStore.Operations.AddLocalOperation(
                operationId,
                OperationType.DebugPackage,
                "Created debug package for current server only",
                detailedDescription: null,
                async _ =>
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        try
                        {
                            var localEndpointClient = new LocalEndpointClient(Server);

                            if (debugInfoType.HasFlag(DebugInfoPackageContentType.ServerWide))
                            {
                                await WriteServerInfo(archive, context, localEndpointClient, token.Token);
                            }

                            if (debugInfoType.HasFlag(DebugInfoPackageContentType.Databases))
                            {
                                await WriteForAllLocalDatabases(archive, context, localEndpointClient, databases, token.Token);
                            }

                            if (debugInfoType.HasFlag(DebugInfoPackageContentType.LogFile))
                            {
                                await WriteLogFile(archive, token.Token);
                            }
                        }
                        catch (Exception e)
                        {
                            await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, $"Node - [{ServerStore.NodeTag}]");
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }

                return null;
            }, token: token);
        }

        private DebugInfoPackageContentType GetDebugInfoPackageContentType()
        {
            var type = GetStringQueryString("type", required: false);
            if (string.IsNullOrEmpty(type))
                return DebugInfoPackageContentType.Default;

            if (Enum.TryParse(type, out DebugInfoPackageContentType debugInfoType) == false)
                throw new ArgumentException($"Query string '{type}' was not recognized as valid type");

            return debugInfoType;
        }

        private static async Task WriteLogFile(ZipArchive archive, CancellationToken token)
        {
            var prefix = $"{_serverWidePrefix}/{DateTime.UtcNow:yyyy-MM-dd H-mm-ss}.log";
            var entry = archive.CreateEntry(prefix, CompressionLevel.Optimal);
            entry.ExternalAttributes = (int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR) << 16;
            await using (var entryStream = entry.Open())
            await using (var sw = new StreamWriter(entryStream))
            {
                token.ThrowIfCancellationRequested();

                try
                {
                    using (StreamTarget.Register(entryStream))
                    {
                        await Task.Delay(15000, token);
                    }
                }
                catch (Exception e)
                {
                    await sw.WriteAsync($"{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffffffZ}, {e.Message}");
                }
                finally
                {
                    await entryStream.FlushAsync();
                }
            }
        }

        private async Task<Stream> GetDebugInfoFromNodeAsync(JsonOperationContext context, RequestExecutor requestExecutor, long operationId, OperationCancelToken token, int timeoutInSec, StringValues databases, DebugInfoPackageContentType contentType)
        {
            var url = $"/admin/debug/info-package?operationId={operationId}";
            if (contentType != DebugInfoPackageContentType.Default)
                url += $"&type={contentType}";

            if (databases.Count > 0)
            {
                foreach (string database in databases)
                    url += $"&database={Uri.EscapeDataString(database)}";
            }

            var rawStreamCommand = new GetRawStreamResultCommand(url);
            var requestExecutionTask = requestExecutor.ExecuteAsync(rawStreamCommand, context);

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token.Token))
            {
                try
                {
                    var delayTask = Task.Delay(TimeSpan.FromSeconds(timeoutInSec), cts.Token);
                    var result = await Task.WhenAny(requestExecutionTask, delayTask);

                    if (result == delayTask)
                    {
                        await KillOperationAsync();
                    }
                    else
                    {
                        cts.Cancel();
                    }
                }
                catch (OperationCanceledException)
                {
                    await KillOperationAsync();
                }
            }

            await requestExecutionTask;

            rawStreamCommand.Result.Position = 0;
            return rawStreamCommand.Result;

            async Task KillOperationAsync()
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var killOperation = new KillServerOperationCommand(operationId);
                    await requestExecutor.ExecuteAsync(killOperation, ctx);
                }
            }
        }

        private async Task WriteForAllLocalDatabases(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, StringValues databases, CancellationToken token)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var databaseName in GetDatabases())
                {
                    using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                    {
                        if (rawRecord == null)
                            continue;

                        if (rawRecord.IsSharded)
                        {
                            if (rawRecord.Sharding.Orchestrator.Topology.RelevantFor(ServerStore.NodeTag) == false)
                                continue;
                        }
                        else
                        {
                            if (rawRecord.Topology.RelevantFor(ServerStore.NodeTag) == false)
                                continue;
                        }

                        await WriteDatabaseRecord(archive, databaseName, jsonOperationContext, context);

                        if (rawRecord.IsDisabled ||
                            rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress ||
                            IsDatabaseBeingDeleted(ServerStore.NodeTag, rawRecord))
                            continue;

                        await WriteDatabaseInfo(archive, jsonOperationContext, localEndpointClient, databaseName, token);
                    }
                }

                HashSet<string> GetDatabases()
                {
                    var allDatabases = ServerStore.Cluster.GetDatabaseNames(context);

                    if (databases.Count > 0)
                    {
                        return allDatabases.Intersect(databases).ToHashSet();
                    }

                    return allDatabases.ToHashSet();
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

        private async Task WriteDatabaseInfo(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, string databaseName, CancellationToken token = default)
        {
            var endpointParameters = new Dictionary<string, StringValues>
            {
                { "database", new StringValues(databaseName) }
            };
            await WriteForServerOrDatabase(archive, jsonOperationContext, localEndpointClient, RouteInformation.RouteType.Databases, databaseName, databaseName, endpointParameters, token);
        }

        private async Task WriteServerInfo(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, CancellationToken token = default)
        {
            await WriteForServerOrDatabase(archive, jsonOperationContext, localEndpointClient, RouteInformation.RouteType.None, _serverWidePrefix, null, null, token);
        }

        private async Task WriteForServerOrDatabase(ZipArchive archive, JsonOperationContext context, LocalEndpointClient localEndpointClient, RouteInformation.RouteType routeType, string path, string databaseName, Dictionary<string, StringValues> endpointParameters = null, CancellationToken token = default)
        {
            var debugInfoDict = new Dictionary<string, TimeSpan>();

            var routes = DebugInfoPackageUtils.GetAuthorizedRoutes(Server, HttpContext, databaseName).Where(x => x.TypeOfRoute == routeType);

            var id = Guid.NewGuid();
            if (Logger.IsInfoEnabled)
                Logger.Info($"Creating Debug Package '{id}' for '{databaseName ?? "Server"}'.");

            foreach (var route in routes)
            {
                if (token.IsCancellationRequested)
                    return;

                Exception ex = null;
                var sw = Stopwatch.StartNew();

                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Started gathering debug info from '{route.Path}' for Debug Package '{id}'.");

                try
                {
                    await InvokeAndWriteToArchive(archive, context, localEndpointClient, route, path, endpointParameters, token);
                    debugInfoDict[route.Path] = sw.Elapsed;
                }
                catch (Exception e)
                {
                    ex = e;
                    await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path, null));
                }
                finally
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Finished gathering debug info from '{route.Path}' for Debug Package '{id}'. Took: {(int)sw.Elapsed.TotalMilliseconds} ms",
                            ex);
                }
            }

            await DebugInfoPackageUtils.WriteDebugInfoTimesAsZipEntryAsync(debugInfoDict, archive, path);
        }

        internal static async Task InvokeAndWriteToArchive(ZipArchive archive, JsonOperationContext jsonOperationContext, 
            LocalEndpointClient localEndpointClient, RouteInformation route, string path, Dictionary<string, 
                StringValues> endpointParameters = null, CancellationToken token = default)
        {
            try
            {
                var response = await localEndpointClient.InvokeAsync(route, endpointParameters, token);

                var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path, response.ContentType == "text/plain" ? "txt" : "json");
                var entry = archive.CreateEntry(entryName);
                entry.ExternalAttributes = (int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR) << 16;

                // we have the response at this point, not using the cancel token here on purpose
                await using (var entryStream = entry.Open())
                {
                    if (response.ContentType == "text/plain")
                    {
                        await response.Body.CopyToAsync(entryStream);
                    }
                    else
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, entryStream))
                        {
                            var endpointOutput = await jsonOperationContext.ReadForMemoryAsync(response.Body, $"read/local endpoint/{route.Path}");
                            jsonOperationContext.Write(writer, endpointOutput);
                            await writer.FlushAsync();
                        }
                    }

                    await entryStream.FlushAsync();
                }
            }
            catch (Exception e)
            {
                //precaution, ideally this exception should never be thrown
                if (e is InvalidStartOfObjectException)
                    e = new InvalidOperationException(
                        "Expected to find a blittable object as a result of debug endpoint, but found something else (see inner exception for details). This should be investigated as all RavenDB endpoints are supposed to return an object.",
                        e);

                throw;
            }
        }

        private async Task WriteDatabaseRecord(ZipArchive archive, string databaseName, JsonOperationContext jsonOperationContext, TransactionOperationContext transactionCtx)
        {
            var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation("/database-record", databaseName, "json");
            try
            {
                var entry = archive.CreateEntry(entryName);
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                await using (var entryStream = entry.Open())
                await using (var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, entryStream))
                {
                    jsonOperationContext.Write(writer, GetDatabaseRecordForDebugPackage(transactionCtx, databaseName));
                    await writer.FlushAsync();
                    await entryStream.FlushAsync();
                }
            }
            catch (Exception e)
            {
                await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, entryName);
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

            return context.ReadObject(djv, "DatabaseRecord");
        }

        internal sealed class NodeDebugInfoRequestHeader
        {
            public string FromUrl { get; set; }

            public List<string> DatabaseNames { get; set; }
        }

        [Flags]
        public enum DebugInfoPackageContentType
        {
            ServerWide = 0x1,
            Databases = 0x2,
            LogFile = 0x4,

            Default = ServerWide | Databases | LogFile
    }
}
}
