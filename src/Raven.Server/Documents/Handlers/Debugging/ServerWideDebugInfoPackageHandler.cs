using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions.Database;
using Raven.Client.Server;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideDebugInfoPackageHandler : AdminRequestHandler
    {
        private static readonly string[] EmptyStringArray = new string[0];
        private static readonly Logger _log = LoggingSource.Instance.GetLogger<ServerStore>(nameof(ServerWideDebugInfoPackageHandler));

        //this endpoint is intended to be called by /debug/cluster-info-package only
        [RavenAction("/debug/remote-cluster-info-package", "GET")]
        public async Task GetClusterwideInfoPackageForRemote()
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
                        using (var requestHeaderJson = await transactionOperationContext.ReadForMemoryAsync(HttpContext.Request.Body, "remote-cluster-info-package/read request header"))
                        {
                            requestHeader = JsonDeserializationServer.NodeDebugInfoRequestHeader(requestHeaderJson);
                        }

                        await WriteServerWide(archive, jsonOperationContext, localEndpointClient);
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

        [RavenAction("/debug/cluster-info-package", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetClusterwideInfoPackage()
        {
            var contentDisposition = $"attachment; filename=Cluster wide debug-info {DateTime.UtcNow}.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;

            using (var httpClient = new HttpClient())
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);

                        await WriteServerWide(archive, jsonOperationContext, 
                            localEndpointClient, Path.Combine($"Node - [{ServerStore.NodeTag}]","server-wide"));
                        await WriteForAllLocalDatabases(archive, jsonOperationContext, 
                            localEndpointClient, $"Node - [{ServerStore.NodeTag}]");

                        var databaseNames = ServerStore.Cluster.GetDatabaseNames(transactionOperationContext).ToList();

                        //this means no databases are defined in the cluster
                        //in this case just output server-wide endpoints from all cluster nodes
                        if (databaseNames.Count == 0)
                        {
                            var topology = ServerStore.GetClusterTopology(transactionOperationContext);
                            foreach (var tagWithUrl in topology.AllNodes)
                            {
                                if (tagWithUrl.Value.Contains(ServerStore.NodeHttpServerUrl))
                                    continue;

                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(httpClient, jsonOperationContext, archive, tagWithUrl.Key, tagWithUrl.Value);
                                }
                                catch (Exception e)
                                {
                                    WriteExceptionAsZipEntry(e,archive,$"Node - [{tagWithUrl.Key}]");
                                }
                            }
                        }
                        else
                        {
                            var nodeUrlToDatabaseNames = CreateUrlToDatabaseNamesMapping(transactionOperationContext, databaseNames);
                            foreach (var urlToDatabaseNamesMap in nodeUrlToDatabaseNames)
                            {
                                if (urlToDatabaseNamesMap.Key.Contains(ServerStore.NodeHttpServerUrl))
                                    continue; //skip writing local data, we do it separately

                                await WriteDebugInfoPackageForNodeAsync(
                                    httpClient,
                                    jsonOperationContext,
                                    archive,
                                    urlToDatabaseNamesMap.Value.Item2,
                                    urlToDatabaseNamesMap.Key,
                                    urlToDatabaseNamesMap.Value.Item1);
                            }
                        }
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }       

        private async Task WriteDebugInfoPackageForNodeAsync(
            HttpClient httpClient, 
            JsonOperationContext jsonOperationContext, 
            ZipArchive archive, string tag, string url, IEnumerable<string> databaseNames = null)
        {
            using (var responseStream = await GetDebugInfoFromNodeAsync(
                httpClient,
                jsonOperationContext,
                url,
                databaseNames ?? EmptyStringArray))
            {
                var entry = archive.CreateEntry($"Node - [{tag}].zip");
                using (var entryStream = entry.Open())
                {
                    await responseStream.CopyToAsync(entryStream);
                    await entryStream.FlushAsync();
                }
            }
        }

        [RavenAction("/debug/info-package", "GET", IsDebugInformationEndpoint = true)]
        public async Task GetInfoPackage()
        {              
            var contentDisposition = $"attachment; filename=Server wide debug-info {DateTime.UtcNow}.zip";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var ms = new MemoryStream())
                {
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
                    {
                        var localEndpointClient = new LocalEndpointClient(Server);
                        await WriteServerWide(archive, context, localEndpointClient);
                        await WriteForAllLocalDatabases(archive, context, localEndpointClient);
                    }

                    ms.Position = 0;
                    await ms.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        private static void WriteExceptionAsZipEntry(Exception e, ZipArchive archive, string entryName)
        {
            var entry = archive.CreateEntry($"{entryName}.error");
            using (var entryStream = entry.Open())
            using (var sw = new StreamWriter(entryStream))
            {
                sw.Write(e);
                sw.Flush();
            }
        }

        private async Task<Stream> GetDebugInfoFromNodeAsync(
            HttpClient httpClient, 
            JsonOperationContext jsonOperationContext,
            string url, 
            IEnumerable<string> databaseNames)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, $"{url}/debug/remote-cluster-info-package");
            using (var ms = new MemoryStream())
            {
                var bodyJson = new DynamicJsonValue
                {
                    [nameof(NodeDebugInfoRequestHeader.FromUrl)] = ServerStore.NodeHttpServerUrl,
                    [nameof(NodeDebugInfoRequestHeader.DatabaseNames)] = databaseNames
                };

                using (var writer = new BlittableJsonTextWriter(jsonOperationContext, ms))
                {
                    jsonOperationContext.Write(writer, bodyJson);
                    writer.Flush();
                    ms.Flush();

                    ms.Position = 0;
                    request.Content = new StreamContent(ms);
                    var response = await httpClient.SendAsync(request);

                    return await response.Content.ReadAsStreamAsync();
                }
            }
        }

        private async Task WriteServerWide(ZipArchive archive, JsonOperationContext context, LocalEndpointClient localEndpointClient, string prefix = "server-wide")
        {
            //theoretically this could be parallelized,
            //however ZipArchive allows only one archive entry to be open concurrently
            foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.None))
            {
                var entryRoute = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, prefix);
                try
                {
                    var entry = archive.CreateEntry(entryRoute);
                    using (var entryStream = entry.Open())
                    using (var writer = new BlittableJsonTextWriter(context, entryStream))
                    using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, context))
                    {
                        context.Write(writer, endpointOutput);
                        writer.Flush();
                        await entryStream.FlushAsync();
                    }
                }
                catch (Exception e)
                {
                    WriteExceptionAsZipEntry(e,archive,entryRoute.Replace(".json",".error"));
                }
            }
        }

        private async Task WriteForAllLocalDatabases(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, string prefix = null)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
            {                
                foreach (var databaseName in ServerStore.Cluster.GetDatabaseNames(transactionOperationContext))
                {
                    var databaseRecord = ServerStore.Cluster.ReadDatabase(transactionOperationContext, databaseName);

                    if (databaseRecord == null ||
                        databaseRecord.Topology.RelevantFor(ServerStore.NodeTag) == false ||
                        IsDatabaseBeingDeleted(ServerStore.NodeTag, databaseRecord))
                        continue;

                    var path = !string.IsNullOrWhiteSpace(prefix) ? Path.Combine(prefix, databaseName) : databaseName;
                    await WriteForDatabase(archive, jsonOperationContext, localEndpointClient, databaseName, path);
                }
            }
        }

        private static bool IsDatabaseBeingDeleted(string tag, DatabaseRecord databaseRecord)
        {            
            return databaseRecord?.DeletionInProgress != null &&
                                         databaseRecord.DeletionInProgress.TryGetValue(tag, out DeletionInProgressStatus deletionInProgress) &&
                                         deletionInProgress != DeletionInProgressStatus.No;
        }

        private static async Task WriteForDatabase(ZipArchive archive, JsonOperationContext jsonOperationContext, LocalEndpointClient localEndpointClient, string databaseName, string path = null)
        {
            var endpointParameters = new Dictionary<string, StringValues>
            {
                {"database", new StringValues(databaseName)},
            };

            foreach (var route in DebugInfoPackageUtils.Routes.Where(x => x.TypeOfRoute == RouteInformation.RouteType.Databases))
            {
                try
                {
                    var entry = archive.CreateEntry(DebugInfoPackageUtils.GetOutputPathFromRouteInformation(route, path ?? databaseName));
                    using (var entryStream = entry.Open())
                    using (var writer = new BlittableJsonTextWriter(jsonOperationContext, entryStream))
                    {
                        using (var endpointOutput = await localEndpointClient.InvokeAndReadObjectAsync(route, jsonOperationContext, endpointParameters))
                        {
                            jsonOperationContext.Write(writer, endpointOutput);
                            writer.Flush();
                            await entryStream.FlushAsync();
                        }
                    }
                }
                catch (Exception e)
                {
                    WriteExceptionAsZipEntry(e,archive,path ?? databaseName);
                }
            }
        }

        private Dictionary<string, (HashSet<string>, string)> CreateUrlToDatabaseNamesMapping(TransactionOperationContext transactionOperationContext, IEnumerable<string> databaseNames)
        {
            var nodeUrlToDatabaseNames = new Dictionary<string, (HashSet<string>, string)>();
            var clusterTopology = ServerStore.GetClusterTopology(transactionOperationContext);
            foreach (var databaseName in databaseNames)
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(transactionOperationContext, databaseName);            

                var nodeUrlsAndTags = databaseRecord.Topology.AllNodes.Select(tag => (clusterTopology.GetUrlFromTag(tag), tag));
                foreach (var urlAndTag in nodeUrlsAndTags)
                {
                    (HashSet<string>, string) databaseNamesWithNodeTag;
                    if (nodeUrlToDatabaseNames.TryGetValue(urlAndTag.Item1, out databaseNamesWithNodeTag))
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
