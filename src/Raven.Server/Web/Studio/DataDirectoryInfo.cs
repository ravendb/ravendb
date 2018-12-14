using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server.Web.Studio
{
    public class DataDirectoryInfo
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<DataDirectoryInfo>("DataDirectoryInfo");

        private readonly ServerStore _serverStore;
        private readonly string _path;
        private readonly bool _getNodesInfo;
        private readonly Stream _responseBodyStream;

        public DataDirectoryInfo(ServerStore serverStore, string path, bool getNodesInfo, Stream responseBodyStream)
        {
            _serverStore = serverStore;
            _path = path;
            _getNodesInfo = getNodesInfo;
            _responseBodyStream = responseBodyStream;
        }

        public async Task UpdateDirectoryResult(string urlPath, string databaseName)
        {
            var drivesInfo = PlatformDetails.RunningOnPosix ? DriveInfo.GetDrives() : null;
            var driveInfo = DiskSpaceChecker.GetDriveInfo(_path, drivesInfo, out var realPath);
            var diskSpaceInfo = DiskSpaceChecker.GetDiskSpaceInfo(driveInfo.DriveName);

            var currentNodeInfo = new SingleNodeDataDirectoryResult
            {
                NodeTag = _serverStore.NodeTag,
                FullPath = realPath,
                TotalFreeSpaceHumane = diskSpaceInfo?.TotalFreeSpace.ToString()
            };

            if (_getNodesInfo == false)
            {
                // write info of a single node
                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, _responseBodyStream))
                {
                    context.Write(writer, currentNodeInfo.ToJson());
                }

                return;
            }

            var clusterTopology = _serverStore.GetClusterTopology();
            var relevantNodes = GetRelevantNodes(databaseName, clusterTopology);

            var dataDirectoryResult = new DataDirectoryResult();
            dataDirectoryResult.List.Add(currentNodeInfo);

            if (relevantNodes.Count > 1)
            {
                await UpdateNodesDirectoryResult(relevantNodes, clusterTopology, urlPath, dataDirectoryResult);
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _responseBodyStream))
            {
                context.Write(writer, dataDirectoryResult.ToJson());
            }
        }

        private List<string> GetRelevantNodes(string databaseName, ClusterTopology clusterTopology)
        {
            if (databaseName == null)
                return clusterTopology.AllNodes.Keys.ToList();

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var rawDatabaseRecord = _serverStore.Cluster.ReadRawDatabase(context, databaseName, out _);
                if (rawDatabaseRecord == null)
                    return new List<string>();

                var databaseTopology = _serverStore.Cluster.ReadDatabaseTopology(rawDatabaseRecord);
                if (databaseTopology == null)
                    return new List<string>();

                return databaseTopology.AllNodes.ToList();
            }
        }

        private async Task UpdateNodesDirectoryResult(
            IEnumerable<string> nodes, ClusterTopology clusterTopology,
            string urlPath, DataDirectoryResult dataDirectoryResult)
        {
            var httpClient = RequestExecutor.CreateHttpClient(_serverStore.Server.Certificate.Certificate, DocumentConventions.Default);
            var tasks = new List<Task<SingleNodeDataDirectoryResult>>();

            foreach (var nodeTag in nodes)
            {
                _serverStore.ServerShutdown.ThrowIfCancellationRequested();

                if (nodeTag.Equals(_serverStore.NodeTag, StringComparison.OrdinalIgnoreCase))
                    continue;

                var serverUrl = clusterTopology.GetUrlFromTag(nodeTag);
                if (serverUrl == null)
                    continue;

                tasks.Add(Task.Run(async () =>
                {
                    var url = $"{serverUrl}/{urlPath}";
                    var singleNodeResult = await GetSingleNodeDataDirectoryInfo(url, httpClient);
                    return singleNodeResult;
                }, _serverStore.ServerShutdown));
            }

            await Task.WhenAll(tasks);

            foreach (var task in tasks)
            {
                if (task.IsCompletedSuccessfully == false)
                    continue;

                var singleNodeResult = await task;
                if (singleNodeResult == null)
                    continue;

                dataDirectoryResult.List.Add(singleNodeResult);
            }
        }

        private async Task<SingleNodeDataDirectoryResult> GetSingleNodeDataDirectoryInfo(string url, HttpClient httpClient)
        {
            using (var cts = new CancellationTokenSource(7 * 1000))
            {
                try
                {
                    var request = new HttpRequestMessage(HttpMethod.Get, url);
                    var response = await httpClient.SendAsync(request, cts.Token);
                    if (response.IsSuccessStatusCode == false)
                        return null;

                    using (var responseStream = await response.Content.ReadAsStreamAsync())
                    using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var singleNodeDataBlittable = await context.ReadForMemoryAsync(responseStream, "studio-tasks-full-data-directory");
                        return JsonDeserializationServer.SingleNodeDataDirectoryResult(singleNodeDataBlittable);
                    }
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to get directory info result from: {url}", e);

                    return null;
                }
            }
        }
    }

    public class SingleNodeDataDirectoryResult : IDynamicJson
    {
        public string NodeTag { get; set; }

        public string FullPath { get; set; }

        public string TotalFreeSpaceHumane { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(FullPath)] = FullPath,
                [nameof(TotalFreeSpaceHumane)] = TotalFreeSpaceHumane
            };
        }
    }

    public class DataDirectoryResult : IDynamicJson
    {
        public DataDirectoryResult()
        {
            List = new List<SingleNodeDataDirectoryResult>();
        }

        public List<SingleNodeDataDirectoryResult> List;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(List)] = TypeConverter.ToBlittableSupportedType(List)
            };
        }
    }
}
