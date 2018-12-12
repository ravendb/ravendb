using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.Studio
{
    public class DataDirectoryInfo
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<DataDirectoryInfo>("DataDirectoryInfo");

        private readonly ServerStore _serverStore;
        private readonly JsonOperationContext _context;

        public DataDirectoryInfo(ServerStore serverStore, JsonOperationContext context)
        {
            _serverStore = serverStore;
            _context = context;
        }

        public async Task<DataDirectoryResult> GetDatabaseDirectoryResult(
            SingleNodeDataDirectoryResult currentNodeInfo, string path, string name)
        {
            var clusterTopology = _serverStore.GetClusterTopology();
            var relevantNodes = clusterTopology.AllNodes.Keys;
            var urlPath = $"admin/studio-tasks/full-data-directory?path={path}&name={name}";

            var dataDirectoryResult = new DataDirectoryResult();
            dataDirectoryResult.List.Add(currentNodeInfo);
            
            if (relevantNodes.Count > 1)
            {
                await UpdateNodesDirectoryResult(relevantNodes, clusterTopology, urlPath, dataDirectoryResult);
            }

            return dataDirectoryResult;
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
                    {
                        var singleNodeDataBlittable = await _context.ReadForMemoryAsync(responseStream, "studio-tasks-full-data-directory");
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
