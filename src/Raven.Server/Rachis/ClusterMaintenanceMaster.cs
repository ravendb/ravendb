using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Collections.LockFree;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Rachis
{
    public partial class ClusterMaintenanceMaster : IDisposable
    {
        private readonly string _leaderClusterTag;

        //maintenance handler is valid for specific term, otherwise it's requests will be rejected by nodes
        private readonly long _term; 
        private bool _isDisposed;

        private readonly ConcurrentSet<ClusterNode> _clusterNodes = new ConcurrentSet<ClusterNode>();
        private readonly ConcurrentDictionary<ClusterNode, long> _failedNodes = new ConcurrentDictionary<ClusterNode, long>();
        private readonly ConcurrentDictionary<ClusterNode, long> _timedOutNodes = new ConcurrentDictionary<ClusterNode, long>();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly JsonContextPool _contextPool = new JsonContextPool();

        public ClusterMaintenanceMaster(string leaderClusterTag, long term)
        {
            _leaderClusterTag = leaderClusterTag;
            _term = term;
        }

        public async Task AddToCluster(string clusterTag, string url)
        {
            var connectionInfo = await ReplicationUtils.GetTcpInfoAsync(MultiDatabase.GetRootDatabaseUrl(url), null, null);
            var clusterNode = new ClusterNode(clusterTag, connectionInfo, _contextPool, this, _cts.Token);
            _clusterNodes.Add(clusterNode);
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
                    node.Dispose();
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
            private readonly ClusterMaintenanceMaster _parent;
            private readonly CancellationToken _token;
            private readonly Logger _log;

            public Exception LastError { get; private set; }

            public string ClusterTag { get; }

            private readonly TcpClient _tcpClient;

            public ClusterNodeStatusReport LastReceivedStatus { get; private set; }

            public DateTime LastUpdateDateTime { get; private set; }

            public DateTime LastSuccessfulUpdateDateTime { get; private set; }

            private bool _isDisposed;
            private readonly string _readStatusUpdateDebugString;

            public Status LastUpdateStatus { get; private set; }

            public enum Status
            {
                Timeout,
                Error,
                Ok
            }

            public ClusterNode(
                string clusterTag, 
                TcpConnectionInfo tcpConnectionInfo,
                JsonContextPool contextPool,
                ClusterMaintenanceMaster parent,
                CancellationToken token)
            {
                ClusterTag = clusterTag;
                _contextPool = contextPool;
                _parent = parent;
                _token = token;
                _readStatusUpdateDebugString = $"ClusterMaintenanceServer/{ClusterTag}/UpdateState/Read-Response";
                _tcpClient = new TcpClient();

                _log = LoggingSource.Instance.GetLogger<ClusterNode>(clusterTag);
                LastUpdateStatus = Status.Ok;

                Task.Factory.StartNew(async () =>
                {
                    Stream connection = null;
                    try
                    {
                        connection = await ConnectToClientNodeAsync(tcpConnectionInfo);
                        while (_token.IsCancellationRequested == false)
                        {
                            BlittableJsonReaderObject statusUpdateJson = null;
                            try
                            {
                                Task<BlittableJsonReaderObject> readResponseTask;
                                using (contextPool.AllocateOperationContext(out JsonOperationContext context))
                                {
                                    var timeout = Task.Delay(5000);
                                    readResponseTask = context.ReadForMemoryAsync(connection, _readStatusUpdateDebugString, _token);
                                    LastUpdateDateTime = DateTime.UtcNow;
                                    if (await Task.WhenAny(readResponseTask, timeout) == timeout)
                                    {
                                        //TODO : logging about timeout
                                        LastUpdateStatus = Status.Timeout;
                                        continue;
                                    }

                                    if (readResponseTask.IsFaulted)
                                    {
                                        //TODO : logging about error
                                        LastUpdateStatus = Status.Error;
                                        LastError = readResponseTask.Exception;

                                        continue;
                                    }
                                }
                                statusUpdateJson = readResponseTask.Result;
                                LastSuccessfulUpdateDateTime = DateTime.UtcNow;

                                LastReceivedStatus = JsonDeserializationServer.ClusterNodeStatusReport(statusUpdateJson);
                            }
                            catch (SocketException e)
                            {
                                //most likely this will never happen, precaution
                                LastError = e;
                                //TODO: log about socket error -> this is most likely will indicate issue with network or remote node going down...
                                LastUpdateStatus = Status.Error;
                                //socket exception means that we have unrecoverable error, so try to reconnect
                                connection.Dispose();

                                await Task.Delay(1000);
                                connection = await ConnectToClientNodeAsync(tcpConnectionInfo);
                            }
                            catch (Exception e)
                            {
                                //TODO : do not forget to add this to log as well
                                LastError = e;
                                LastUpdateStatus = Status.Error;
                                await Task.Delay(1000); //wait a bit in case of transient error
                            }
                            finally
                            {
                                statusUpdateJson?.Dispose();
                            }
                        }
                    }
                    finally
                    {
                        connection?.Dispose();
                    }
                }, _token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            }         


            private async Task<Stream> ConnectAndGetNetworkStreamAsync(TcpConnectionInfo tcpConnectionInfo)
            {
                await TcpUtils.ConnectSocketAsync(tcpConnectionInfo, _tcpClient, _log, _token);
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
                    _tcpClient?.Dispose();
                }
                catch
                {
                    //don't care, we are disposing
                }
                GC.SuppressFinalize(this);
            }

            ~ClusterNode()
            {
                //TODO : add logging about running finalizer where we shouldn't
                Dispose();
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
        }
    }
}
