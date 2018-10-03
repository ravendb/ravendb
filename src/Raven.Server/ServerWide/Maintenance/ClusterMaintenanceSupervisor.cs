using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

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

        public ClusterMaintenanceSupervisor(ServerStore server, string leaderClusterTag, long term)
        {
            _leaderClusterTag = leaderClusterTag;
            _term = term;
            _server = server;
            Config = server.Configuration.Cluster;
        }

        public void AddToCluster(string clusterTag, string url)
        {
            var clusterNode = new ClusterNode(clusterTag, url, _term, _contextPool, this, _cts.Token);
            _clusterNodes[clusterTag] = clusterNode;
            clusterNode.Start();
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

            Parallel.ForEach(_clusterNodes, (node) =>
            {
                try
                {
                    node.Value.Dispose();
                }
                catch
                {
                    //don't care, we are disposing
                }
            });

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
            private readonly string _name;
            private PoolOfThreads.LongRunningWork _maintenanceTask;

            public ClusterNode(
                string clusterTag,
                string url,
                long term,
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
                _readStatusUpdateDebugString = $"ClusterMaintenanceServer/{ClusterTag}/UpdateState/Read-Response in term {term}";
                _name = $"Maintenance supervisor from {_parent._server.NodeTag} to {ClusterTag} in term {term}";
                _log = LoggingSource.Instance.GetLogger<ClusterNode>(_name);
            }

            public void Start()
            {
                _maintenanceTask = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
                {
                    try
                    {
                        ListenToMaintenanceWorker();
                    }
                    catch (ObjectDisposedException)
                    {
                        // expected
                    }
                    catch (OperationCanceledException)
                    {
                        // expected
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Exception occurred while collecting info from {ClusterTag}. Task is closed.", e);
                        }
                        // we don't want to crash the process so we don't propagate this exception.
                    }
                }, null, _name);
            }

            private void ListenToMaintenanceWorker()
            {
                var firstIteration = true;
                var onErrorDelayTime = _parent.Config.OnErrorDelayTime.AsTimeSpan;
                var receiveFromWorkerTimeout = _parent.Config.ReceiveFromWorkerTimeout.AsTimeSpan;
                var tcpTimeout = _parent.Config.TcpConnectionTimeout.AsTimeSpan;
                
                if (tcpTimeout < receiveFromWorkerTimeout)
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info(
                            $"Warning: TCP timeout is lower than the receive from worker timeout ({tcpTimeout} < {receiveFromWorkerTimeout}), " +
                            "this could affect the cluster observer's decisions.");
                    }
                }

                while (_token.IsCancellationRequested == false)
                {
                    try
                    {
                        if (firstIteration == false) 
                        {
                            // avoid tight loop if there was timeout / error
                            _token.WaitHandle.WaitOne(onErrorDelayTime);
                            if (_token.IsCancellationRequested)
                                return;
                        }
                        firstIteration = false;

                        TcpConnectionInfo tcpConnection = null;
                        using (var timeout = new CancellationTokenSource(tcpTimeout))
                        using (var combined = CancellationTokenSource.CreateLinkedTokenSource(_token, timeout.Token))
                        {
                            tcpConnection = ReplicationUtils.GetTcpInfo(Url, null, "Supervisor", _parent._server.Server.Certificate.Certificate, combined.Token);
                            if (tcpConnection == null)
                            {
                                continue;
                            }
                        }

                        var connection = ConnectToClientNode(tcpConnection, _parent._server.Engine.TcpConnectionTimeout);
                        var tcpClient = connection.TcpClient;
                        var stream = connection.Stream;
                        using (tcpClient)
                        using (_cts.Token.Register(tcpClient.Dispose))
                        using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
                        using (var timeoutEvent = new TimeoutEvent(receiveFromWorkerTimeout, $"Timeout event for: {_name}", singleShot: false))
                        {
                            timeoutEvent.Start(OnTimeout);
                            while (_token.IsCancellationRequested == false)
                            {
                                BlittableJsonReaderObject rawReport;
                                try
                                {
                                    // even if there is a timeout event, we will keep waiting on the same connection until the TCP timeout occurs.
                                    rawReport = context.ReadForMemory(stream, _readStatusUpdateDebugString);
                                    timeoutEvent.Defer(_parent._leaderClusterTag);
                                }
                                catch (Exception e)
                                {
                                    if (_token.IsCancellationRequested)
                                    {
                                        return;
                                    }

                                    if (_log.IsInfoEnabled)
                                    {
                                        _log.Info("Exception occurred while reading the report from the connection", e);
                                    }

                                    ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(),
                                        ClusterNodeStatusReport.ReportStatus.Error,
                                        e,
                                        DateTime.UtcNow,
                                        _lastSuccessfulReceivedReport);

                                    break;
                                }

                                var report = BuildReport(rawReport);
                                timeoutEvent.Defer(_parent._leaderClusterTag);
                                foreach (var name in report.Report.Keys.ToList())
                                {
                                    var dbReport = report.Report[name];
                                    if (dbReport.Status == DatabaseStatus.NoChange)
                                    {
                                        report.Report[name] = _lastSuccessfulReceivedReport.Report[name];
                                        report.Report[name].UpTime = dbReport.UpTime;
                                    }
                                }
                                
                                ReceivedReport = _lastSuccessfulReceivedReport = report;
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
                    }
                }
            }

            private void OnTimeout()
            {
                if (_token.IsCancellationRequested)
                    return;

                // expected timeout
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Timeout occurred while collecting info report.");
                }

                ReceivedReport = new ClusterNodeStatusReport(new Dictionary<string, DatabaseStatusReport>(),
                    ClusterNodeStatusReport.ReportStatus.Timeout,
                    null,
                    DateTime.UtcNow,
                    _lastSuccessfulReceivedReport);
            }

            private ClusterNodeStatusReport BuildReport(BlittableJsonReaderObject rawReport)
            {
                using (rawReport)
                {
                    var report = new Dictionary<string, DatabaseStatusReport>();
                    foreach (var property in rawReport.GetPropertyNames())
                    {
                        var value = (BlittableJsonReaderObject)rawReport[property];
                        report.Add(property, JsonDeserializationServer.DatabaseStatusReport(value));
                    }

                    return new ClusterNodeStatusReport(
                        report,
                        ClusterNodeStatusReport.ReportStatus.Ok,
                        null,
                        DateTime.UtcNow,
                        _lastSuccessfulReceivedReport);
                }
            }

            private ClusterMaintenanceConnection ConnectToClientNode(TcpConnectionInfo tcpConnectionInfo, TimeSpan timeout)
            {
                return AsyncHelpers.RunSync(() => ConnectToClientNodeAsync(tcpConnectionInfo, timeout));
            }

            private async Task<ClusterMaintenanceConnection> ConnectToClientNodeAsync(TcpConnectionInfo tcpConnectionInfo, TimeSpan timeout)
            {
                TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures;
                var tcpClient = await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, timeout, _log);
                var connection = await TcpUtils.WrapStreamWithSslAsync(tcpClient, tcpConnectionInfo, _parent._server.Server.Certificate.Certificate, timeout);
                using (_contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                using (var writer = new BlittableJsonTextWriter(ctx, connection))
                {
                    var parameters = new TcpNegotiateParameters
                    {
                        Database = null,
                        Operation = TcpConnectionHeaderMessage.OperationTypes.Maintenance,
                        Version = TcpConnectionHeaderMessage.MaintenanceTcpVersion,
                        ReadResponseAndGetVersionCallback = SupervisorReadResponseAndGetVersion,
                        DestinationUrl = tcpConnectionInfo.Url,
                        DestinationNodeTag = ClusterTag
                        
                    };
                    supportedFeatures = TcpNegotiation.NegotiateProtocolVersion(ctx, connection, parameters);

                    WriteClusterMaintenanceConnectionHeader(writer);
                }

                return new ClusterMaintenanceConnection
                {
                    TcpClient = tcpClient,
                    Stream = connection,
                    SupportedFeatures = supportedFeatures
                };
            }

            private int SupervisorReadResponseAndGetVersion(JsonOperationContext ctx, BlittableJsonTextWriter writer, Stream stream, string url)
            {
                using (var responseJson = ctx.ReadForMemory(stream, _readStatusUpdateDebugString + "/Read-Handshake-Response"))
                {
                    var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                    switch (headerResponse.Status)
                    {
                        case TcpConnectionStatus.Ok:
                            return headerResponse.Version;
                        case TcpConnectionStatus.AuthorizationFailed:
                            throw new AuthorizationException(
                                $"Node with ClusterTag = {ClusterTag} replied to initial handshake with authorization failure {headerResponse.Message}");
                        case TcpConnectionStatus.TcpVersionMismatch:
                            if (headerResponse.Version != TcpNegotiation.OutOfRangeStatus)
                            {
                                return headerResponse.Version;
                            }
                            //Kindly request the server to drop the connection
                            WriteOperationHeaderToRemote(writer, headerResponse.Version, drop: true);
                            throw new InvalidOperationException($"Node with ClusterTag = {ClusterTag} replied to initial handshake with mismatching tcp version {headerResponse.Message}");
                        default:
                            throw new InvalidOperationException($"{url} replied with unknown status {headerResponse.Status}, message:{headerResponse.Message}");
                    }
                }
            }

            private void WriteOperationHeaderToRemote(BlittableJsonTextWriter writer, int remoteVersion = -1, bool drop = false)
            {
                var operation = drop ? TcpConnectionHeaderMessage.OperationTypes.Drop : TcpConnectionHeaderMessage.OperationTypes.Maintenance;
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
                    writer.WriteString(operation.ToString());
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.OperationVersion));
                    writer.WriteInteger(TcpConnectionHeaderMessage.MaintenanceTcpVersion);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                    writer.WriteString((string)null);
                    if (drop)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Info));
                        writer.WriteString($"Couldn't agree on heartbeats tcp version ours:{TcpConnectionHeaderMessage.MaintenanceTcpVersion} theirs:{remoteVersion}");
                    }
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
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;
                return other.GetType() == GetType() && Equals((ClusterNode)other);
            }

            public override int GetHashCode() => ClusterTag?.GetHashCode() ?? 0;

            public void Dispose()
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;
                _cts.Cancel();

                try
                {
                    if (_maintenanceTask == null)
                        return;

                    if (_maintenanceTask.ManagedThreadId == Thread.CurrentThread.ManagedThreadId)
                        return;

                    if (_maintenanceTask.Join((int)TimeSpan.FromSeconds(30).TotalMilliseconds) == false)
                    {
                        throw new ObjectDisposedException($"{_name} still running and can't be closed");
                    }
                }
                finally
                {
                    _cts.Dispose();
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
