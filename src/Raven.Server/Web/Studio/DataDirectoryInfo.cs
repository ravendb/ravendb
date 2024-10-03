﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Utils;

namespace Raven.Server.Web.Studio
{
    public sealed class DataDirectoryInfo
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<DataDirectoryInfo>();

        private readonly ServerStore _serverStore;
        private readonly string _path;
        private readonly string _name;
        private readonly bool _isBackup;
        private readonly bool _getNodesInfo;
        private readonly int _requestTimeoutInMs;
        private readonly Stream _responseBodyStream;

        public DataDirectoryInfo(
            ServerStore serverStore, string path, string name, bool isBackup,
            bool getNodesInfo, int requestTimeoutInMs, Stream responseBodyStream)
        {
            _serverStore = serverStore;
            _path = path;
            _name = name;
            _isBackup = isBackup;
            _getNodesInfo = getNodesInfo;
            _requestTimeoutInMs = requestTimeoutInMs;
            _responseBodyStream = responseBodyStream;
        }

        public async Task UpdateDirectoryResult(string databaseName, string error)
        {
            var drivesInfo = PlatformDetails.RunningOnPosix ? DriveInfo.GetDrives() : null;
            var driveInfo = DiskUtils.GetDriveInfo(_path, drivesInfo, out var realPath);
            var diskSpaceInfo = DiskUtils.GetDiskSpaceInfo(driveInfo.DriveName, driveInfo);

            if (CanAccessPath(_path, out var pathAccessError) == false)
                error = pathAccessError;

            var currentNodeDirectoryInfo = new SingleNodeDataDirectoryResult
            {
                NodeTag = _serverStore.NodeTag,
                FullPath = realPath,
                FreeSpaceInBytes = diskSpaceInfo?.TotalFreeSpace.GetValue(SizeUnit.Bytes) ?? 0,
                FreeSpaceHumane = diskSpaceInfo?.TotalFreeSpace.ToString(),
                TotalSpaceInBytes = diskSpaceInfo?.TotalSize.GetValue(SizeUnit.Bytes) ?? 0,
                TotalSpaceHumane = diskSpaceInfo?.TotalSize.ToString(),
                Error = error
            };

            if (_getNodesInfo == false)
            {
                // write info of a single node
                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, _responseBodyStream))
                {
                    context.Write(writer, currentNodeDirectoryInfo.ToJson());
                }

                return;
            }

            var clusterTopology = _serverStore.GetClusterTopology();
            var relevantNodes = GetRelevantNodes(databaseName, clusterTopology);

            var dataDirectoryResult = new DataDirectoryResult();
            dataDirectoryResult.List.Add(currentNodeDirectoryInfo);

            if (relevantNodes.Count > 1)
            {
                await UpdateNodesDirectoryResult(relevantNodes, clusterTopology, dataDirectoryResult);
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, _responseBodyStream))
            {
                context.Write(writer, dataDirectoryResult.ToJson());
            }
        }

        public static bool CanAccessPath(string folderPath, out string error)
        {
            var originalFolderPath = folderPath;
            while (true)
            {
                var directoryInfo = new DirectoryInfo(folderPath);
                if (directoryInfo.Exists == false)
                {
                    if (directoryInfo.Parent == null)
                    {
                        error = $"Path {originalFolderPath} cannot be accessed " +
                                $"because '{folderPath}' doesn't exist";
                        return false;
                    }

                    folderPath = directoryInfo.Parent.FullName;
                    continue;
                }

                if (directoryInfo.Attributes.HasFlag(FileAttributes.ReadOnly))
                {
                    error = $"Cannot write to directory path: {originalFolderPath}";
                    return false;
                }

                break;
            }

            error = null;
            return true;
        }

        private List<string> GetRelevantNodes(string databaseName, ClusterTopology clusterTopology)
        {
            if (databaseName == null)
                return clusterTopology.AllNodes.Keys.ToList();

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
            {
                if (rawRecord == null)
                    return new List<string>();

                var databaseTopology = rawRecord.Topology;
                if (databaseTopology == null)
                    return new List<string>();

                return databaseTopology.AllNodes.ToList();
            }
        }

        private async Task UpdateNodesDirectoryResult(IEnumerable<string> nodes, ClusterTopology clusterTopology, DataDirectoryResult dataDirectoryResult)
        {
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
                    var singleNodeResult = await GetSingleNodeDataDirectoryInfo(serverUrl);
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

        private async Task<SingleNodeDataDirectoryResult> GetSingleNodeDataDirectoryInfo(string serverUrl)
        {
            using (var cts = new CancellationTokenSource(_requestTimeoutInMs))
            {
                try
                {
                    using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(serverUrl, _serverStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
                    using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var dataDirectoryInfo = new GetDataDirectoryInfoCommand(_path, _name, _isBackup);
                        await requestExecutor.ExecuteAsync(dataDirectoryInfo, context, token: cts.Token);

                        return dataDirectoryInfo.Result;
                    }
                }
                catch (OperationCanceledException)
                {
                    return null;
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to get directory info result from: {serverUrl}", e);

                    return null;
                }
            }
        }

        internal sealed class GetDataDirectoryInfoCommand : RavenCommand<SingleNodeDataDirectoryResult>
        {
            private readonly string _path;
            private readonly string _name;
            private readonly bool _isBackup;

            public GetDataDirectoryInfoCommand(string path, string name, bool isBackup)
            {
                _path = path;
                _name = name;
                _isBackup = isBackup;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var encodedPath = _path != null ? Uri.EscapeDataString(_path) : "";
                var path = _isBackup
                    ? $"databases/{_name}/admin/backup-data-directory?path={encodedPath}"
                    : $"admin/studio-tasks/full-data-directory?path={encodedPath}&name={_name}";

                url = $"{node.Url}/{path}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationServer.SingleNodeDataDirectoryResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }

    public sealed class SingleNodeDataDirectoryResult : IDynamicJson
    {
        public string NodeTag { get; set; }

        public string FullPath { get; set; }

        public long FreeSpaceInBytes { get; set; }

        public string FreeSpaceHumane { get; set; }

        public long TotalSpaceInBytes { get; set; }

        public string TotalSpaceHumane { get; set; }

        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(FullPath)] = FullPath,
                [nameof(FreeSpaceInBytes)] = FreeSpaceInBytes,
                [nameof(FreeSpaceHumane)] = FreeSpaceHumane,
                [nameof(TotalSpaceInBytes)] = TotalSpaceInBytes,
                [nameof(TotalSpaceHumane)] = TotalSpaceHumane,
                [nameof(Error)] = Error
            };
        }
    }

    public sealed class DataDirectoryResult : IDynamicJson
    {
        public DataDirectoryResult()
        {
            List = new SortedSet<SingleNodeDataDirectoryResult>(new DataDirectoryComparer());
        }

        public SortedSet<SingleNodeDataDirectoryResult> List;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(List)] = TypeConverter.ToBlittableSupportedType(List)
            };
        }

        private sealed class DataDirectoryComparer : IComparer<SingleNodeDataDirectoryResult>
        {
            public int Compare(SingleNodeDataDirectoryResult x, SingleNodeDataDirectoryResult y)
            {
                Debug.Assert(x != null && y != null);

                return string.Compare(x.NodeTag, y.NodeTag, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
