using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterMaintenanceSupervisor : IDisposable
    {
        private readonly string _leaderClusterTag;

        //maintenance handler is valid for specific term, otherwise it's requests will be rejected by nodes
        private readonly long _term;
        private bool _isDisposed;

        private readonly ConcurrentDictionary<string, ClusterNode> _clusterNodes = new ConcurrentDictionary<string, ClusterNode>();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly JsonContextPool _contextPool = new JsonContextPool();

        internal readonly ClusterConfiguration Config;
        public ClusterMaintenanceSupervisor(ServerStore server,string leaderClusterTag, long term)
        {
            _leaderClusterTag = leaderClusterTag;
            _term = term;
            Config = server.Configuration.Cluster;
        }

        public async Task AddToCluster(string clusterTag, string url)
        {
            var connectionInfo = await ReplicationUtils.GetTcpInfoAsync(MultiDatabase.GetRootDatabaseUrl(url), null, null, "Supervisor");

            var clusterNode = new ClusterNode(clusterTag, connectionInfo, _contextPool, this, _cts.Token);
            _clusterNodes[clusterTag] = clusterNode;
            var task = clusterNode.StartListening();
            GC.KeepAlive(task); // we are explicitly not waiting on this task
        }

        public Dictionary<string, ClusterNodeStatusReport> GetStats()
        {
            var clusterStats = new Dictionary<string, ClusterNodeStatusReport>();
            foreach (var node in _clusterNodes)
            {
                clusterStats[node.Key] = node.Value.ReceivedReport;
            }
            return clusterStats;
        }

        public void RemoveFromCluster(string clusterTag)
        {
            if (_clusterNodes.TryRemove(clusterTag, out ClusterNode node))
            {
                node.Dispose();
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            _cts.Cancel();

            foreach (var node in _clusterNodes)
            {
                try
                {
                    node.Value.Dispose();
                }
                catch
                {
                    //don't care, we are disposing
                }
            }

            try
            {
                _contextPool.Dispose();
            }
            catch
            {
                //don't care -> disposing
            }
        }

        public class ClusterNode : IDisposable
        {
            private readonly JsonContextPool _contextPool;
            private readonly ClusterMaintenanceSupervisor _parent;
            private readonly CancellationToken _token;
            private readonly CancellationTokenSource _cts;

            private readonly Logger _log;

            public string ClusterTag { get; }

            private TcpClient _tcpClient;

            public ClusterNodeStatusReport ReceivedReport = new ClusterNodeStatusReport(
                new Dictionary<string, DatabaseStatusReport>(), ClusterNodeStatusReport.ReportStatus.WaitingForResponse,
                null, DateTime.MinValue, DateTime.MinValue
                );
            private DateTime _lastSuccessfulUpdateDateTime;
            private bool _isDisposed;
            private readonly string _readStatusUpdateDebugString;
            private readonly TcpConnectionInfo _tcpConnection;
            public ClusterNode(
                string clusterTag,
                TcpConnectionInfo tcpConnectionConnectionInfo,
                JsonContextPool contextPool,
                ClusterMaintenanceSupervisor parent,
                CancellationToken token)
            {
                ClusterTag = clusterTag;
                _contextPool = contextPool;
                _parent = parent;
                _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
                _token = _cts.Token;
                _readStatusUpdateDebugString = $"ClusterMaintenanceServer/{ClusterTag}/UpdateState/Read-Response";
                _tcpClient = new TcpClient();

                _log = LoggingSource.Instance.GetLogger<ClusterNode>(clusterTag);
                _tcpConnection = tcpConnectionConnectionInfo;
            }

            public Task StartListening()
            {
                return ListenToMaintenanceWorker();
            }

            private async Task ListenToMaintenanceWorker()
            {
                bool needToWait = false;
                var onErrorDelayTime = _parent.Config.OnErrorDelayTime.AsTimeSpan;
                var receiveFromWorkerTimeout = _parent.Config.ReceiveFromWorkerTimeout.AsTimeSpan;

                while (_token.IsCancellationRequested == false)
                {
                    try
                    {
                        if (needToWait)
                        {
                            needToWait = false; // avoid tight loop if there was timeout / error
                            await TimeoutManager.WaitFor(onErrorDelayTime, _token);

                            if (_tcpClient.Connected == false)
                            {
                                _tcpClient?.Dispose();
                                _tcpClient = new TcpClient();
                            }
                        }
                        using (var connection = await ConnectToClientNodeAsync(_tcpConnection))
                        {
                            while (_token.IsCancellationRequested == false)
                            {
                                using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
                                {                                    
                                    var readResponseTask = context.ReadForMemoryAsync(connection, _readStatusUpdateDebugString, _token);
                                    var timeout = TimeoutManager.WaitFor(receiveFromWorkerTimeout, _token);

                                    if (await Task.WhenAny(readResponseTask, timeout) == timeout)
                                    {
                                        if (_log.IsInfoEnabled)
                                        {
                                            _log.Info($"Timeout occurred while collection info from {ClusterTag}");
                                        }
                                        ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(), 
                                            ClusterNodeStatusReport.ReportStatus.Timeout,
                                            null,
                                            DateTime.UtcNow,
                                            _lastSuccessfulUpdateDateTime);
                                        await readResponseTask;
                                        needToWait = true;
                                        break;
                                    }

                                    using (var statusUpdateJson = await readResponseTask)
                                    {
                                        var report = new Dictionary<string, DatabaseStatusReport>();
                                        foreach (var property in statusUpdateJson.GetPropertyNames())
                                        {
                                            var value = (BlittableJsonReaderObject)statusUpdateJson[property];
                                            report.Add(property, JsonDeserializationServer.DatabaseStatusReport(value));
                                        }
                                        _lastSuccessfulUpdateDateTime = DateTime.Now;
                                        
                                        ReceivedReport = new ClusterNodeStatusReport(
                                            report,
                                            ClusterNodeStatusReport.ReportStatus.Ok,
                                            null,
                                            DateTime.UtcNow,
                                            _lastSuccessfulUpdateDateTime);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Exception was thrown while collection info from {ClusterTag}", e);
                        }
                        ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(), 
                            ClusterNodeStatusReport.ReportStatus.Error,
                            e,
                            DateTime.UtcNow,
                            _lastSuccessfulUpdateDateTime);
                        needToWait = true;
                    }
                }
            }

            private async Task<Stream> ConnectAndGetNetworkStreamAsync(TcpConnectionInfo tcpConnectionInfo)
            {
                await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, _tcpClient, _log);
                return await TcpUtils.WrapStreamWithSslAsync(_tcpClient, tcpConnectionInfo);
            }

            private async Task<Stream> ConnectToClientNodeAsync(TcpConnectionInfo tcpConnectionInfo)
            {
                var connection = await ConnectAndGetNetworkStreamAsync(tcpConnectionInfo);
                using (_contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (var writer = new BlittableJsonTextWriter(ctx, connection))
                {
                    WriteOperationHeaderToRemote(writer);
                    using (var responseJson = await ctx.ReadForMemoryAsync(connection, _readStatusUpdateDebugString + "/Read-Handshake-Response", _token))
                    {
                        var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                        switch (headerResponse.Status)
                        {
                            case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                                //All good nothing to do
                                break;
                            default:
                                throw new UnauthorizedAccessException(
                                    $"Node with ClusterTag = {ClusterTag} replied to initial handshake with authorization failure {headerResponse.Status}");
                        }
                    }

                    WriteClusterMaintenanceConnectionHeader(writer);
                }

                return connection;
            }

            private void WriteOperationHeaderToRemote(BlittableJsonTextWriter writer)
            {
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
                    writer.WriteString(TcpConnectionHeaderMessage.OperationTypes.Heartbeats.ToString());
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                    writer.WriteString((string)null);
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.AuthorizationToken));
                    writer.WriteString((string)null);//TODO: fixme
                }
                writer.WriteEndObject();
                writer.Flush();
            }

            private void WriteClusterMaintenanceConnectionHeader(BlittableJsonTextWriter writer)
            {
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(ClusterMaintenanceConnectionHeader.LeaderClusterTag));
                    writer.WriteString(_parent._leaderClusterTag);
                    writer.WritePropertyName(nameof(ClusterMaintenanceConnectionHeader.Term));
                    writer.WriteInteger(_parent._term);
                }
                writer.WriteEndObject();
                writer.Flush();
            }

            protected bool Equals(ClusterNode other)
            {
                return string.Equals(ClusterTag, other.ClusterTag);
            }

            public override bool Equals(object other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return other.GetType() == GetType() && Equals((ClusterNode)other);
            }

            public override int GetHashCode() => ClusterTag?.GetHashCode() ?? 0;

            public void Dispose()
            {
                if (_isDisposed)
                    return;
                _isDisposed = true;
                try
                {
                    _cts.Cancel();
                    _tcpClient?.Dispose();
                }
                catch
                {
                    //don't care, we are disposing
                }
            }

        }

        public enum StateUpdateResult
        {
            Timeout = 1,
            OutdatedTerm = 2,
            Error = 3,
            Ok = 4
        }

        public class ClusterMaintenanceConnectionHeader
        {
            public string LeaderClusterTag { get; set; }

            public long Term { get; set; }
        }
    }
}
