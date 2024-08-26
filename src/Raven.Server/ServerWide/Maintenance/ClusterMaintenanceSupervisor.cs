using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.ServerWide.Maintenance
{
    public sealed class ClusterMaintenanceSupervisor : IDisposable
    {
        private readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForCluster<ClusterMaintenanceSupervisor>();

        private readonly string _leaderClusterTag;

        //maintenance handler is valid for specific term, otherwise it's requests will be rejected by nodes
        private readonly long _term;

        private bool _isDisposed;

        private readonly ConcurrentDictionary<string, ClusterNode> _clusterNodes = new ConcurrentDictionary<string, ClusterNode>();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly JsonContextPool _contextPool;

        internal readonly ClusterConfiguration Config;
        private readonly ServerStore _server;
        internal ServerStore ServerStore => _server;

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action<ClusterNode> NoChangeFoundAction;
            internal Action<ClusterNode> BeforeReportBuildAction;
            internal Action<ClusterNode> AfterSettingReportAction;

            private TriggerState _currentState;
            private ClusterNode _currentClusterNode;

            internal void SetTriggerTimeoutAfterNoChangeAction(string tag)
            {
                AfterSettingReportAction = BeforeReportBuildAction = NoChangeFoundAction = (node) =>
                {
                    if (node.ClusterTag != tag)
                        return;

                    if (_currentClusterNode != node)
                    {
                        _currentClusterNode = node;
                        _currentState = TriggerState.None;
                    }

                    switch (_currentState)
                    {
                        case TriggerState.None:
                            _currentState = TriggerState.Ready;
                            break;
                        case TriggerState.Ready:
                            _currentState = TriggerState.Armed;
                            break;
                        case TriggerState.Armed:
                            node.OnTimeout();
                            _currentState = TriggerState.Fired;
                            break;
                        case TriggerState.Fired:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                };
            }
            private enum TriggerState
            {
                None,
                Ready,
                Armed,
                Fired
            }
        }

        public ClusterMaintenanceSupervisor(ServerStore server, string leaderClusterTag, long term)
        {
            _leaderClusterTag = leaderClusterTag;
            _term = term;
            _server = server;
            _contextPool = new JsonContextPool(server.Configuration.Memory.MaxContextSizeToKeep, Logger);
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

        public sealed class ClusterNode : IDisposable
        {
            private readonly JsonContextPool _contextPool;
            private readonly ClusterMaintenanceSupervisor _parent;
            private readonly CancellationToken _token;
            private readonly CancellationTokenSource _cts;

            private readonly RavenLogger _log;

            public string ClusterTag { get; }
            public string Url { get; }

            public ClusterNodeStatusReport ReceivedReport = new ClusterNodeStatusReport(
                new ServerReport(),
                new Dictionary<string, DatabaseStatusReport>(),
                ClusterNodeStatusReport.ReportStatus.WaitingForResponse,
                null, DateTime.MinValue, null);

            private bool _isDisposed;
            private readonly string _readStatusUpdateDebugString;
            private ClusterNodeStatusReport _lastSuccessfulReceivedReport;
            private readonly string _name;
            private PoolOfThreads.LongRunningWork _maintenanceTask;
            private long _term;

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
                _name = $"Heartbeats supervisor from {_parent._server.NodeTag} to {ClusterTag} in term {term}";
                _log = RavenLogManager.Instance.GetLoggerForCluster<ClusterNode>(LoggingComponent.Name(_name));
                _term = term;
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
                }, null, ThreadNames.ForHeartbeatsSupervisor(_name, _parent._server.NodeTag, ClusterTag, _term));
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
                            tcpConnection = ReplicationUtils.GetServerTcpInfo(Url, "Supervisor", _parent._server.Server.Certificate.Certificate, combined.Token);
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
                        using (_contextPool.AllocateOperationContext(out JsonOperationContext contextForParsing))
                        using (_contextPool.AllocateOperationContext(out JsonOperationContext contextForBuffer))
                        using (contextForBuffer.GetMemoryBuffer(out var readBuffer))
                        using (var timeoutEvent = new TimeoutEvent(receiveFromWorkerTimeout, $"Timeout event for: {_name}", singleShot: false))
                        {
                            timeoutEvent.Start(OnTimeout);
                            var unchangedReports = new List<DatabaseStatusReport>();

                            while (_token.IsCancellationRequested == false)
                            {
                                contextForParsing.Reset();
                                contextForParsing.Renew();
                                BlittableJsonReaderObject rawReport;
                                try
                                {
                                    // even if there is a timeout event, we will keep waiting on the same connection until the TCP timeout occurs.

                                    rawReport = contextForParsing.Sync.ParseToMemory(stream, _readStatusUpdateDebugString, BlittableJsonDocumentBuilder.UsageMode.None, readBuffer);
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

                                    ReceivedReport = new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>(),
                                        ClusterNodeStatusReport.ReportStatus.Error,
                                        e,
                                        DateTime.UtcNow,
                                        _lastSuccessfulReceivedReport);

                                    break;
                                }

                                _parent.ForTestingPurposes?.BeforeReportBuildAction(this);

                                var nodeReport = BuildReport(rawReport, connection.SupportedFeatures);
                                timeoutEvent.Defer(_parent._leaderClusterTag);


                                UpdateNodeReportIfNeeded(nodeReport, unchangedReports);
                                unchangedReports.Clear();

                                ReceivedReport = _lastSuccessfulReceivedReport = nodeReport;
                                _parent.ForTestingPurposes?.AfterSettingReportAction(this);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (_token.IsCancellationRequested)
                            return;

                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Exception was thrown while collecting info from {ClusterTag}", e);
                        }

                        ReceivedReport = new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>(),
                            ClusterNodeStatusReport.ReportStatus.Error,
                            e,
                            DateTime.UtcNow,
                            _lastSuccessfulReceivedReport);
                    }
                }
            }

            private void UpdateNodeReportIfNeeded(ClusterNodeStatusReport nodeReport, List<DatabaseStatusReport> unchangedReports)
            {
                foreach (var dbReport in nodeReport.Report)
                {
                    if (dbReport.Value.Status == DatabaseStatus.NoChange)
                    {
                        _parent.ForTestingPurposes?.NoChangeFoundAction(this);

                        unchangedReports.Add(dbReport.Value);
                    }
                }

                if (unchangedReports.Count == 0)
                    return;

                // we take the last received and not the last successful.
                // we don't want to reuse by mistake a successful report when we receive an 'unchanged' error.
                var lastReport = ReceivedReport;
                switch (lastReport!.Status)
                {
                    case ClusterNodeStatusReport.ReportStatus.WaitingForResponse:
                    case ClusterNodeStatusReport.ReportStatus.Timeout:
                    case ClusterNodeStatusReport.ReportStatus.Error:
                        throw new InvalidOperationException($"We have databases with '{DatabaseStatus.NoChange}' status, but our last report from this node is '{lastReport.Status}'");
                    case ClusterNodeStatusReport.ReportStatus.Ok:
                    case ClusterNodeStatusReport.ReportStatus.OutOfCredits:
                    case ClusterNodeStatusReport.ReportStatus.EarlyOutOfMemory:
                    case ClusterNodeStatusReport.ReportStatus.HighDirtyMemory:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($"The status {lastReport.Status} is not supported.");
                }

                foreach (var dbReport in unchangedReports)
                {
                    var dbName = dbReport.Name;
                    if (lastReport.Report.TryGetValue(dbName, out var previous) == false)
                    {
                        throw new InvalidOperationException(
                            $"We got '{DatabaseStatus.NoChange}' for the database '{dbReport}', but it is missing in the last good report");
                    }

                    previous.LastSentEtag = dbReport.LastSentEtag;
                    previous.LastCompareExchangeIndex = dbReport.LastCompareExchangeIndex;
                    previous.LastCompletedClusterTransaction = dbReport.LastCompletedClusterTransaction;
                    previous.LastClusterWideTransactionRaftIndex = dbReport.LastClusterWideTransactionRaftIndex;
                    previous.UpTime = dbReport.UpTime;
                    nodeReport.Report[dbName] = previous;
                }
            }

            internal void OnTimeout()
            {
                if (_token.IsCancellationRequested)
                    return;

                // expected timeout
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Timeout occurred while collecting info report.");
                }

                ReceivedReport = new ClusterNodeStatusReport(new ServerReport(), new Dictionary<string, DatabaseStatusReport>(),
                    ClusterNodeStatusReport.ReportStatus.Timeout,
                    null,
                    DateTime.UtcNow,
                    _lastSuccessfulReceivedReport);
            }

            private ClusterNodeStatusReport BuildReport(BlittableJsonReaderObject rawReport, TcpConnectionHeaderMessage.SupportedFeatures supportedFeatures)
            {
                using (rawReport)
                {
                    if (supportedFeatures.Heartbeats.IncludeServerInfo)
                    {
                        var maintenanceReport = JsonDeserializationServer.MaintenanceReport(rawReport);

                        return new ClusterNodeStatusReport(
                            maintenanceReport.ServerReport,
                            maintenanceReport.DatabasesReport,
                            GetStatus(),
                            null,
                            DateTime.UtcNow,
                            _lastSuccessfulReceivedReport);

                        ClusterNodeStatusReport.ReportStatus GetStatus()
                        {
                            if (maintenanceReport.ServerReport.OutOfCpuCredits == true)
                                return ClusterNodeStatusReport.ReportStatus.OutOfCredits;

                            if (maintenanceReport.ServerReport.EarlyOutOfMemory == true)
                                return ClusterNodeStatusReport.ReportStatus.EarlyOutOfMemory;

                            if (maintenanceReport.ServerReport.HighDirtyMemory == true)
                                return ClusterNodeStatusReport.ReportStatus.HighDirtyMemory;

                            return ClusterNodeStatusReport.ReportStatus.Ok;
                        }
                    }

                    var report = new Dictionary<string, DatabaseStatusReport>();
                    foreach (var property in rawReport.GetPropertyNames())
                    {
                        var value = (BlittableJsonReaderObject)rawReport[property];
                        report.Add(property, JsonDeserializationServer.DatabaseStatusReport(value));
                    }

                    return new ClusterNodeStatusReport(
                        new ServerReport(),
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
                Stream connection;
                TcpClient tcpClient;

                using (_contextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var result = await TcpUtils.ConnectSecuredTcpSocket(
                        tcpConnectionInfo,
                        _parent._server.Server.Certificate.Certificate,
                        _parent._server.Server.CipherSuitesPolicy,
                        TcpConnectionHeaderMessage.OperationTypes.Heartbeats,
                        NegotiateProtocolVersionAsyncForClusterSupervisor,
                        ctx,
                        timeout, null, _token);

                    tcpClient = result.TcpClient;
                    connection = result.Stream;
                    supportedFeatures = result.SupportedFeatures;

                    if (result.SupportedFeatures.DataCompression)
                    {
                        connection = new ReadWriteCompressedStream(connection);
                    }

                    await WriteClusterMaintenanceConnectionHeaderAsync(connection, ctx);
                }

                return new ClusterMaintenanceConnection
                {
                    TcpClient = tcpClient,
                    Stream = connection,
                    SupportedFeatures = supportedFeatures
                };
            }

            private async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateProtocolVersionAsyncForClusterSupervisor(string url, TcpConnectionInfo info, Stream stream, JsonOperationContext context, List<string> _)
            {
                bool compressionSupport = false;
                var version = TcpConnectionHeaderMessage.HeartbeatsTcpVersion;
                if (version >= TcpConnectionHeaderMessage.HeartbeatsWithTcpCompression)
                    compressionSupport = true;

                var parameters = new AsyncTcpNegotiateParameters
                {
                    Database = null,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Heartbeats,
                    Version = TcpConnectionHeaderMessage.HeartbeatsTcpVersion,
                    ReadResponseAndGetVersionCallbackAsync = SupervisorReadResponseAndGetVersionAsync,
                    DestinationUrl = url,
                    DestinationNodeTag = ClusterTag,
                    DestinationServerId = info.ServerId,
                    LicensedFeatures = new LicensedFeatures
                    {
                        DataCompression = compressionSupport && _parent.ServerStore.LicenseManager.LicenseStatus.HasTcpDataCompression &&_parent.ServerStore.Configuration.Server.DisableTcpCompression == false
                    }
                };
                return await TcpNegotiation.NegotiateProtocolVersionAsync(context, stream, parameters);
            }

            private async ValueTask<TcpConnectionHeaderMessage.NegotiationResponse> SupervisorReadResponseAndGetVersionAsync(JsonOperationContext ctx, AsyncBlittableJsonTextWriter writer, Stream stream, string url)
            {
                using (var responseJson = await ctx.ReadForMemoryAsync(stream, _readStatusUpdateDebugString + "/Read-Handshake-Response"))
                {
                    var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(responseJson);
                    switch (headerResponse.Status)
                    {
                        case TcpConnectionStatus.Ok:
                            return new TcpConnectionHeaderMessage.NegotiationResponse
                            {
                                Version = headerResponse.Version,
                                LicensedFeatures = headerResponse.LicensedFeatures
                            };
                        case TcpConnectionStatus.AuthorizationFailed:
                            throw new AuthorizationException(
                                $"Node with ClusterTag = {ClusterTag} replied to initial handshake with authorization failure {headerResponse.Message}");
                        case TcpConnectionStatus.TcpVersionMismatch:
                            if (headerResponse.Version != TcpNegotiation.OutOfRangeStatus)
                            {
                                return new TcpConnectionHeaderMessage.NegotiationResponse
                                {
                                    Version = headerResponse.Version,
                                    LicensedFeatures = headerResponse.LicensedFeatures
                                };
                            }
                            //Kindly request the server to drop the connection
                            await WriteOperationHeaderToRemoteAsync(writer, headerResponse.Version, drop: true);
                            throw new InvalidOperationException($"Node with ClusterTag = {ClusterTag} replied to initial handshake with mismatching tcp version {headerResponse.Message}");
                        case TcpConnectionStatus.InvalidNetworkTopology:
                            throw new AuthorizationException(
                                $"Node with ClusterTag = {ClusterTag} replied to initial handshake with {nameof(TcpConnectionStatus.InvalidNetworkTopology)} error {headerResponse.Message}");
                        default:
                            throw new InvalidOperationException($"{url} replied with unknown status {headerResponse.Status}, message:{headerResponse.Message}");
                    }
                }
            }

            private static async ValueTask WriteOperationHeaderToRemoteAsync(AsyncBlittableJsonTextWriter writer, int remoteVersion = -1, bool drop = false)
            {
                var operation = drop ? TcpConnectionHeaderMessage.OperationTypes.Drop : TcpConnectionHeaderMessage.OperationTypes.Heartbeats;
                writer.WriteStartObject();
                {
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Operation));
                    writer.WriteString(operation.ToString());
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.OperationVersion));
                    writer.WriteInteger(TcpConnectionHeaderMessage.HeartbeatsTcpVersion);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.DatabaseName));
                    writer.WriteString((string)null);
                    if (drop)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TcpConnectionHeaderMessage.Info));
                        writer.WriteString($"Couldn't agree on heartbeats tcp version ours:{TcpConnectionHeaderMessage.HeartbeatsTcpVersion} theirs:{remoteVersion}");
                    }
                }
                writer.WriteEndObject();
                await writer.FlushAsync();
            }

            private async ValueTask WriteClusterMaintenanceConnectionHeaderAsync(Stream stream, JsonOperationContext context)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ClusterMaintenanceConnectionHeader.LeaderClusterTag)] = _parent._leaderClusterTag,
                        [nameof(ClusterMaintenanceConnectionHeader.Term)] = _parent._term
                    });
                }
            }

            private bool Equals(ClusterNode other)
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

        public sealed class ClusterMaintenanceConnectionHeader
        {
            public string LeaderClusterTag { get; set; }

            public long Term { get; set; }
        }
    }
}
