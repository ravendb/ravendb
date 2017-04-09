using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http.OAuth;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class NodeTopologyExplorer : IDisposable
    {
        private readonly DocumentsContextPool _pool;
        private readonly List<string> _alreadyVisited;
        private readonly string _dbId;
        private readonly long _timeout;
        private readonly TcpClient _tcpClient;
        private readonly Logger _log;
        private readonly ApiKeyAuthenticator _authenticator = new ApiKeyAuthenticator();
        private TcpConnectionInfo _tcpConnectionInfo;
        public ReplicationNode Node => _destination;
        private readonly ReplicationNode _destination;
        public NodeTopologyExplorer(
            DocumentsContextPool pool,
            List<string> alreadyVisited,
            ReplicationNode node,
            string dbId,
            TimeSpan timeout)
        {
            _pool = pool;
            _alreadyVisited = alreadyVisited;
            _destination = node;
            _dbId = dbId;
            _timeout = (long)Math.Max(5000, timeout.TotalMilliseconds - 10000);// reduce the timeout by 10 sec each hop, to a min of 5
            _log = LoggingSource.Instance.GetLogger<NodeTopologyExplorer>(node.Database);
            _tcpClient = new TcpClient();
        }

        

        public async Task<FullTopologyInfo> DiscoverTopologyAsync()
        {
            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            {
                //TODO:
                _tcpConnectionInfo = await ReplicationUtils.GetTcpInfoAsync(_destination.Url, _destination.Database,null);
                var token = await _authenticator.GetAuthenticationTokenAsync(null, _destination.Url, context);
                await ConnectSocketAsync();
                using (var stream = await TcpUtils.WrapStreamWithSslAsync(_tcpClient, _tcpConnectionInfo))
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _destination.Database,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.TopologyDiscovery.ToString(),
                        [nameof(TcpConnectionHeaderMessage.AuthorizationToken)] = token
                    });
                    writer.Flush();
                    JsonOperationContext.ManagedPinnedBuffer buffer;
                    using (context.GetManagedBuffer(out buffer))
                    {
                        await ReadTcpHeaderResponseAndThrowOnUnauthorized(context, stream, buffer);

                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(TopologyDiscoveryRequest.OriginDbId)] = _dbId,
                            [nameof(TopologyDiscoveryRequest.Timeout)] = _timeout,
                            [nameof(TopologyDiscoveryRequest.AlreadyVisited)] = new DynamicJsonArray(_alreadyVisited),
                        });

                        writer.Flush();

                        TopologyDiscoveryResponseHeader topologyResponse;

                        using (var topologyResponseJson =
                                await context.ParseToMemoryAsync(stream, "ReplicationDiscovere/Read-topology-response",
                                    BlittableJsonDocumentBuilder.UsageMode.None,
                                    buffer))
                        {
                            topologyResponseJson.BlittableValidation();
                            topologyResponse = JsonDeserializationServer.TopologyDiscoveryResponse(topologyResponseJson);
                        }

                        if (topologyResponse.Type == TopologyDiscoveryResponseHeader.Status.AlreadyKnown)
                            return null;

                        if (topologyResponse.Type == TopologyDiscoveryResponseHeader.Status.Error)
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info("Discover topology request failed. Reason:" + topologyResponse.Exception);
                            throw new InvalidOperationException(topologyResponse.Message, new InvalidOperationException(topologyResponse.Exception));
                        }
                        using (var topologyInfoJson = await context.ParseToMemoryAsync(stream, "ReplicationDiscovere/Read-topology-info",
                            BlittableJsonDocumentBuilder.UsageMode.None,
                            buffer))
                        {
                            topologyInfoJson.BlittableValidation();
                            var topology = JsonDeserializationServer.FullTopologyInfo(topologyInfoJson);
                            return topology;
                        }
                    }
                }
            }
        }

        private async Task ReadTcpHeaderResponseAndThrowOnUnauthorized(JsonOperationContext context, Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            using (var tcpConnectionHeaderResponse =
                await context.ParseToMemoryAsync(stream, "ReplicationDiscovere/tcpConnectionHeaderResponse",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    buffer))
            {
                tcpConnectionHeaderResponse.BlittableValidation();
                var headerResponse = JsonDeserializationServer.TcpConnectionHeaderResponse(tcpConnectionHeaderResponse);
                switch (headerResponse.Status)
                {
                    case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                        //All good nothing to do
                        break;
                    default:
                        throw new UnauthorizedAccessException($"{_destination.Url}/{_destination.Database} replied with failure {headerResponse.Status}");
                }
            }
        }

        public void Dispose()
        {
            _tcpClient.Dispose();
        }

        private async Task ConnectSocketAsync()
        {
            var uri = new Uri(_tcpConnectionInfo.Url);
            var host = uri.Host;
            var port = uri.Port;
            try
            {
                await _tcpClient.ConnectAsync(host, port);
            }
            catch (SocketException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {_tcpConnectionInfo.Url} for topology discovery. Socket Error Code = {e.SocketErrorCode}", e);
                throw;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {_tcpConnectionInfo.Url}  for topology discovery.", e);
                throw;
            }
        }
    }
}
