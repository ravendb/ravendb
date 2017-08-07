using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
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

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class ServerWideDebugInfoPackageHandler : RequestHandler
    {
        private static readonly string[] EmptyStringArray = new string[0];

        //this endpoint is intended to be called by /debug/cluster-info-package only
        [RavenAction("/admin/debug/remote-cluster-info-package", "GET", AuthorizationStatus.ServerAdmin)]
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

        [RavenAction("/admin/debug/cluster-info-package", "GET", AuthorizationStatus.ServerAdmin, IsDebugInformationEndpoint = true)]
        public async Task GetClusterwideInfoPackage()
        {
            var contentDisposition = $"attachment; filename=Cluster wide debug-info {DateTime.UtcNow}.zip";

            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;            

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
                        var topology = ServerStore.GetClusterTopology(transactionOperationContext);
                        
                        //this means no databases are defined in the cluster
                        //in this case just output server-wide endpoints from all cluster nodes
                        if (databaseNames.Count == 0)
                        {                            
                            foreach (var tagWithUrl in topology.AllNodes)
                            {
                                if (tagWithUrl.Value.Contains(ServerStore.NodeHttpServerUrl))
                                    continue;

                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(
                                        jsonOperationContext, 
                                        archive, 
                                        tag: tagWithUrl.Key, 
                                        url: tagWithUrl.Value,
                                        certificate: Server.ServerCertificateHolder.Certificate,
                                        databaseNames:null);
                                }
                                catch (Exception e)
                                {
                                    var entryName = $"Node - [{tagWithUrl.Key}]";
                                    DebugInfoPackageUtils.WriteExceptionAsZipEntry(e,archive,entryName);
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

                                try
                                {
                                    await WriteDebugInfoPackageForNodeAsync(
                                        jsonOperationContext,
                                        archive,
                                        tag: urlToDatabaseNamesMap.Value.Item2,
                                        url: urlToDatabaseNamesMap.Key,
                                        databaseNames: urlToDatabaseNamesMap.Value.Item1,
                                        certificate: Server.ServerCertificateHolder.Certificate);
                                }
                                catch (Exception e)
                                {
                                    var entryName = $"Node - [{urlToDatabaseNamesMap.Value.Item2}]";
                                    DebugInfoPackageUtils.WriteExceptionAsZipEntry(e,archive,entryName);
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
            JsonOperationContext jsonOperationContext, 
            ZipArchive archive, 
            string tag, 
            string url, 
            IEnumerable<string> databaseNames, 
            X509Certificate2 certificate)
        {
            //note : theoretically GetDebugInfoFromNodeAsync() can throw, error handling is done at the level of WriteDebugInfoPackageForNodeAsync() calls
            using (var responseStream = await GetDebugInfoFromNodeAsync(
                jsonOperationContext,
                url,
                databaseNames ?? EmptyStringArray, certificate))
            {
                var entry = archive.CreateEntry($"Node - [{tag}].zip");
                using (var entryStream = entry.Open())
                {
                    await responseStream.CopyToAsync(entryStream);
                    await entryStream.FlushAsync();
                }
            }
        }

        [RavenAction("/admin/debug/info-package", "GET", AuthorizationStatus.ServerAdmin, IsDebugInformationEndpoint = true)]
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

        private async Task<Stream> GetDebugInfoFromNodeAsync(JsonOperationContext jsonOperationContext, 
            string url, IEnumerable<string> databaseNames, X509Certificate2 certificate)
        {
            var bodyJson = new DynamicJsonValue
            {
                [nameof(NodeDebugInfoRequestHeader.FromUrl)] = ServerStore.NodeHttpServerUrl,
                [nameof(NodeDebugInfoRequestHeader.DatabaseNames)] = databaseNames
            };

            using (var ms = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(jsonOperationContext, ms))
            {
                jsonOperationContext.Write(writer, bodyJson);
                writer.Flush();
                ms.Flush();

                var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(url, certificate);
                requestExecutor.DefaultTimeout = ServerStore.Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan;

                var rawStreamCommand = new GetRawStreamResultCommand("/debug/remote-cluster-info-package", ms);

                await requestExecutor.ExecuteAsync(rawStreamCommand, jsonOperationContext);
                return rawStreamCommand.Result;
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
                    DebugInfoPackageUtils.WriteExceptionAsZipEntry(e,archive,entryRoute);
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
                    DebugInfoPackageUtils.WriteExceptionAsZipEntry(e,archive,path ?? databaseName);
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
