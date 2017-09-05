using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Config.Categories;
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
        private readonly ServerStore _server;

        public ClusterMaintenanceSupervisor(ServerStore server,string leaderClusterTag, long term)
        {
            _leaderClusterTag = leaderClusterTag;
            _term = term;
            _server = server;
            Config = server.Configuration.Cluster;
        }

        public void AddToCluster(string clusterTag, string url)
        {
            var clusterNode = new ClusterNode(clusterTag, url, _contextPool, this, _cts.Token);
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
            public string Url { get; }

            public ClusterNodeStatusReport ReceivedReport = new ClusterNodeStatusReport(
                new Dictionary<string, DatabaseStatusReport>(), ClusterNodeStatusReport.ReportStatus.WaitingForResponse,
                null, DateTime.MinValue, null);

            private bool _isDisposed;
            private readonly string _readStatusUpdateDebugString;
            private ClusterNodeStatusReport _lastSuccessfulReceivedReport;

            public ClusterNode(
                string clusterTag,
                string url,
                JsonContextPool contextPool,
                ClusterMaintenanceSupervisor parent,
                CancellationToken token)
            {
                ClusterTag = clusterTag;
                Url = url;
                _contextPool = contextPool;
                _parent = parent;
                _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
                _token = _cts.Token;
                _readStatusUpdateDebugString = $"ClusterMaintenanceServer/{ClusterTag}/UpdateState/Read-Response";
                _log = LoggingSource.Instance.GetLogger<ClusterNode>(clusterTag);
            }

            public async Task StartListening()
            {
                await ListenToMaintenanceWorker();
            }

            private async Task ListenToMaintenanceWorker()
            {
                bool needToWait = false;
                var onErrorDelayTime = _parent.Config.OnErrorDelayTime.AsTimeSpan;
                var receiveFromWorkerTimeout = _parent.Config.ReceiveFromWorkerTimeout.AsTimeSpan;

                TcpConnectionInfo tcpConnection = null;
                try
                {
                    tcpConnection = await ReplicationUtils.GetTcpInfoAsync(Url, null, "Supervisor", 
                        _parent._server.RavenServer.ClusterCertificateHolder?.Certificate);
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"ClusterMaintenanceSupervisor() => Failed to add to cluster node key = {ClusterTag}", e);
                }
                while (_token.IsCancellationRequested == false)
                {
                    var internalTaskCancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_token);
                    try
                    {
                        if (needToWait)
                        {
                            needToWait = false; // avoid tight loop if there was timeout / error
                            await TimeoutManager.WaitFor(onErrorDelayTime, _token);

                            tcpConnection = await ReplicationUtils.GetTcpInfoAsync(Url, null, "Supervisor",
                                _parent._server.RavenServer.ClusterCertificateHolder.Certificate);
                        }

                        if (tcpConnection == null)
                        {
                            needToWait = true;
                            continue;
                        }
                        using (var tcpClient = new TcpClient())
                        using (_cts.Token.Register(tcpClient.Dispose))
                        using (var connection = await ConnectToClientNodeAsync(tcpConnection, tcpClient))
                        {
                            while (_token.IsCancellationRequested == false)
                            {
                                using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
                                {
                                    var readResponseTask = context.ReadForMemoryAsync(connection, _readStatusUpdateDebugString, internalTaskCancellationToken.Token);
                                    var timeout = TimeoutManager.WaitFor(receiveFromWorkerTimeout, _token);

                                    if (await Task.WhenAny(readResponseTask.AsTask(), timeout) == timeout)
                                    {
                                        if (_log.IsInfoEnabled)
                                        {
                                            _log.Info($"Timeout occurred while collecting info from {ClusterTag}");
                                        }
                                        ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(),
                                            ClusterNodeStatusReport.ReportStatus.Timeout,
                                            null,
                                            DateTime.UtcNow,
                                            _lastSuccessfulReceivedReport);
                                        needToWait = true;
                                        internalTaskCancellationToken.Cancel();
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

                                        ReceivedReport = new ClusterNodeStatusReport(
                                            report,
                                            ClusterNodeStatusReport.ReportStatus.Ok,
                                            null,
                                            DateTime.UtcNow,
                                            _lastSuccessfulReceivedReport);
                                        _lastSuccessfulReceivedReport = ReceivedReport;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Exception was thrown while collecting info from {ClusterTag}", e);
                        }
                        ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(),
                            ClusterNodeStatusReport.ReportStatus.Error,
                            e,
                            DateTime.UtcNow,
                            _lastSuccessfulReceivedReport);
                        needToWait = true;
                    }
                    finally
                    {
                        internalTaskCancellationToken.Dispose();
                    }
                }
            }

            private async Task<Stream> ConnectToClientNodeAsync(TcpConnectionInfo tcpConnectionInfo, TcpClient tcpClient)
            {
                TcpUtils.SetTimeouts(tcpClient, _parent._server.Engine.TcpConnectionTimeout);
                await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, tcpClient, _log);
                var connection = await TcpUtils.WrapStreamWithSslAsync(tcpClient, tcpConnectionInfo, _parent._server.RavenServer.ClusterCertificateHolder.Certificate);
                using (_contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (var writer = new BlittableJsonTextWriter(ctx, connection))
                {
                    WriteOperationHeaderToRemote(writer);
                    using (var responseJson = await ctx.ReadForMemoryAsync(connection, _readStatusUpdateDebugString + "/Read-Handshake-Response", _token))
                    {
                        var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                        switch (headerResponse.Status)
                        {
                            case TcpConnectionStatus.Ok:
                                break;
                            case TcpConnectionStatus.AuthorizationFailed:
                                throw new UnauthorizedAccessException(
                                    $"Node with ClusterTag = {ClusterTag} replied to initial handshake with authorization failure {headerResponse.Message}");
                            case TcpConnectionStatus.TcpVersionMissmatch:
                                throw new InvalidOperationException($"Node with ClusterTag = {ClusterTag} replied to initial handshake with missmatching tcp version {headerResponse.Message}");
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
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.OperationVersion));
                    writer.WriteInteger(TcpConnectionHeaderMessage.HeartbeatsTcpVersion);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                    writer.WriteString((string)null);
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
