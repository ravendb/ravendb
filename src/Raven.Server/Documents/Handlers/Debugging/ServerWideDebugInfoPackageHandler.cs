using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideDebugInfoPackageHandler : RequestHandler
    {
        private static readonly string[] EmptyStringArray = new string[0];
        private const string _serverWidePrefix = "server-wide";

        //this endpoint is intended to be called by /debug/cluster-info-package only
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

                        await WriteServerWide(archive, jsonOperationContext, localEndpointClient, _serverWidePrefix);
                        foreach (var databaseName in requestHeader.DatabaseNames)
                        {
                            await WriteForDatabase(archive, jsonOperationContext, localEndpointClient, databaseName);
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
            HttpContext.Response.Headers["Content-Type"] = "application/zip";

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);

                        await using (var localMemoryStream = new MemoryStream())
                        {
                            //assuming that if the name tag is empty
                            var nodeName = $"Node - [{ServerStore.NodeTag ?? "Empty node tag"}]";

                            using (var localArchive = new ZipArchive(localMemoryStream, ZipArchiveMode.Create, true))
                            {
                                await WriteServerWide(localArchive, jsonOperationContext, localEndpointClient, _serverWidePrefix);
                                await WriteForAllLocalDatabases(localArchive, jsonOperationContext, localEndpointClient);
                                await WriteLogFile(localArchive);
                            }

                            localMemoryStream.Position = 0;
                            var entry = archive.CreateEntry($"{nodeName}.zip");
                            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                            await using (var entryStream = entry.Open())
                            {
                                await localMemoryStream.CopyToAsync(entryStream);
                                await entryStream.FlushAsync();
                            }
                        }
                        var databaseNames = ServerStore.Cluster.GetDatabaseNames(transactionOperationContext);
                        var topology = ServerStore.GetClusterTopology(transactionOperationContext);

                        //this means no databases are defined in the cluster
                        //in this case just output server-wide endpoints from all cluster nodes
                        if (databaseNames.Count == 0)
                        {
                            foreach (var tagWithUrl in topology.AllNodes)
                            {
                                if (tagWithUrl.Value.Contains(ServerStore.GetNodeHttpServerUrl()))
                                    continue;

                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(
                                        jsonOperationContext,
                                        archive,
                                        tag: tagWithUrl.Key,
                                        url: tagWithUrl.Value,
                                        certificate: Server.Certificate.Certificate,
                                        databaseNames: null);
                                }
                                catch (Exception e)
                                {
                                    var entryName = $"Node - [{tagWithUrl.Key}]";
                                    await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, entryName);
                                }
                            }
                        }
                        else
                        {
                            var nodeUrlToDatabaseNames = CreateUrlToDatabaseNamesMapping(transactionOperationContext, databaseNames);
                            foreach (var urlToDatabaseNamesMap in nodeUrlToDatabaseNames)
                            {
                                if (urlToDatabaseNamesMap.Key.Contains(ServerStore.GetNodeHttpServerUrl()))
                                    continue; //skip writing local data, we do it separately

                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(
                                        jsonOperationContext,
                                        archive,
                                        tag: urlToDatabaseNamesMap.Value.Item2,
                                        url: urlToDatabaseNamesMap.Key,
                                        databaseNames: urlToDatabaseNamesMap.Value.Item1,
                                        certificate: Server.Certificate.Certificate);
                                }
                                catch (Exception e)
                                {
                                    var entryName = $"Node - [{urlToDatabaseNamesMap.Value.Item2}]";
                                    await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, entryName);
                                }
                            }
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private async Task WriteDebugInfoPackageForNodeAsync(
            JsonOperationContext context,
            ZipArchive archive,
            string tag,
            string url,
            IEnumerable<string> databaseNames,
            X509Certificate2 certificate)
        {
            //note : theoretically GetDebugInfoFromNodeAsync() can throw, error handling is done at the level of WriteDebugInfoPackageForNodeAsync() calls
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, certificate))
            {
                var timeout = TimeSpan.FromMinutes(1);
                if (ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan > timeout)
                    timeout = ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan;

                requestExecutor.DefaultTimeout = timeout;

                await using (var responseStream = await GetDebugInfoFromNodeAsync(
                    context,
                    requestExecutor,
                    databaseNames ?? EmptyStringArray))
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
            var contentDisposition = $"attachment; filename={DateTime.UtcNow:yyyy-MM-dd H:mm:ss} - Node [{ServerStore.NodeTag}].zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.Headers["Content-Type"] = "application/zip";

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        await WriteServerWide(archive, context, localEndpointClient, _serverWidePrefix);
                        await WriteForAllLocalDatabases(archive, context, localEndpointClient);
                        await WriteLogFile(archive);
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private static async Task WriteLogFile(ZipArchive archive)
        {
            var prefix = $"{_serverWidePrefix}/{DateTime.UtcNow:yyyy-MM-dd H:mm:ss}.txt";

            try
            {
                var entry = archive.CreateEntry(prefix, CompressionLevel.Optimal);
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                await using (var entryStream = entry.Open())
                {
                    LoggingSource.Instance.AttachPipeSink(entryStream);

                    await Task.Delay(15000);
                    LoggingSource.Instance.DetachPipeSink();

                    await entryStream.FlushAsync();
                }
            }
            catch (Exception e)
            {
                LoggingSource.Instance.DetachPipeSink();
                await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, prefix);
            }
        }

        private async Task<Stream> GetDebugInfoFromNodeAsync(
            JsonOperationContext context,
            RequestExecutor requestExecutor,
            IEnumerable<string> databaseNames)
        {
            var bodyJson = new DynamicJsonValue
            {
                [nameof(NodeDebugInfoRequestHeader.FromUrl)] = ServerStore.GetNodeHttpServerUrl(),
                [nameof(NodeDebugInfoRequestHeader.DatabaseNames)] = databaseNames
            };

            await using (var ms = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                context.Write(writer, bodyJson);
                await writer.FlushAsync();
                await ms.FlushAsync();

                var rawStreamCommand = new GetRawStreamResultCommand($"admin/debug/remote-cluster-info-package", ms);

                await requestExecutor.ExecuteAsync(rawStreamCommand, context);
                rawStreamCommand.Result.Position = 0;
                return rawStreamCommand.Result;
            }
        }

        private async Task WriteServerWide(ZipArchive archive, JsonOperationContext context, LocalEndpointClient localEndpointClient, string prefix)
        {
            //theoretically this could be parallelized,
            //however ZipArchive allows only one archive entry to be open concurrently
            foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.None))
            {
                var entryRoute = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, prefix);
                try
                {
                    var entry = archive.CreateEntry(entryRoute);
                    entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                    await using (var entryStream = entry.Open())
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, entryStream))
                    using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, context))
                    {
                        context.Write(writer, endpointOutput);
                        await writer.FlushAsync();
                        await entryStream.FlushAsync();
                    }
                }
                catch (Exception e)
                {
                    await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, entryRoute);
                }
            }
        }

        private async Task WriteForAllLocalDatabases(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, string prefix = null)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var databaseName in ServerStore.Cluster.GetDatabaseNames(context))
                {
                    using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                    {
                        if (rawRecord == null ||
                            rawRecord.Topology.RelevantFor(ServerStore.NodeTag) == false ||
                            rawRecord.IsDisabled ||
                            rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress ||
                            IsDatabaseBeingDeleted(ServerStore.NodeTag, rawRecord))
                            continue;
                    }

                    var path = !string.IsNullOrWhiteSpace(prefix) ? Path.Combine(prefix, databaseName) : databaseName;
                    await WriteForDatabase(archive, jsonOperationContext, localEndpointClient, databaseName, path);
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

        private static async Task WriteForDatabase(ZipArchive archive, JsonOperationContext context, LocalEndpointClient localEndpointClient, string databaseName, string path = null)
        {
            var endpointParameters = new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>
            {
                {"database", new Microsoft.Extensions.Primitives.StringValues(databaseName)}
            };

            foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases))
            {
                try
                {
                    var entry = archive.CreateEntry(DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path ?? databaseName));
                    entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                    await using (var entryStream = entry.Open())
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, entryStream))
                    {
                        using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, context, endpointParameters))
                        {
                            context.Write(writer, endpointOutput);
                            await writer.FlushAsync();
                            await entryStream.FlushAsync();
                        }
                    }
                }
                catch (Exception e)
                {
                    await DebugInfoPackageUtils.WriteExceptionAsZipEntryAsync(e, archive, path ?? databaseName);
                }
            }
        }

        private Dictionary<string, (HashSet<string>, string)> CreateUrlToDatabaseNamesMapping(TransactionOperationContext transactionOperationContext, IEnumerable<string> databaseNames)
        {
            var nodeUrlToDatabaseNames = new Dictionary<string, (HashSet<string>, string)>();
            var clusterTopology = ServerStore.GetClusterTopology(transactionOperationContext);
            foreach (var databaseName in databaseNames)
            {
                var topology = ServerStore.Cluster.ReadDatabaseTopology(transactionOperationContext, databaseName);
                var nodeUrlsAndTags = topology.AllNodes.Select(tag => (clusterTopology.GetUrlFromTag(tag), tag));
                foreach (var urlAndTag in nodeUrlsAndTags)
                {
                    if (nodeUrlToDatabaseNames.TryGetValue(urlAndTag.Item1, out (HashSet<string>, string) databaseNamesWithNodeTag))
                    {
                        databaseNamesWithNodeTag.Item1.Add(databaseName);
                    }
                    else
                    {
                        nodeUrlToDatabaseNames.Add(urlAndTag.Item1, (new HashSet<string> { databaseName }, urlAndTag.Item2));
                    }
                }
            }

            return nodeUrlToDatabaseNames;
        }

        internal class NodeDebugInfoRequestHeader
        {
            public string FromUrl { get; set; }

            public List<string> DatabaseNames { get; set; }
        }
    }
}
