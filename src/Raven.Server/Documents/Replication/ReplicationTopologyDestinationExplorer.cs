using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationTopologyDestinationExplorer : IDisposable
    {
        private readonly JsonOperationContext _context;
        private readonly Dictionary<string, List<string>> _alreadyVisited;
        private readonly ReplicationDestination _destination;
        private readonly OperationCredentials _operationCredentials;
        private readonly string _dbId;
        private readonly long _timeout;
        private string _tcpUrl;
        private readonly TcpClient _tcpClient;
        private readonly Logger _log;

        public ReplicationTopologyDestinationExplorer(
            JsonOperationContext context,
            Dictionary<string, List<string>> alreadyVisited, 
            ReplicationDestination destination, 
            OperationCredentials operationCredentials, 
            string dbId,
            long timeout)
        {
            _context = context;
            _alreadyVisited = alreadyVisited;
            _destination = destination;
            _operationCredentials = operationCredentials;
            _dbId = dbId;
            _timeout = Math.Max(5000, timeout - 10000);// reduce the timeout by 10 sec each hop, to a min of 5
            _log = LoggingSource.Instance.GetLogger<ReplicationTopologyDestinationExplorer>(destination.Database);
            _tcpClient = new TcpClient();
        }

        public ReplicationDestination Destination => _destination;

        public async Task<NodeTopologyInfo> DiscoverTopologyAsync()
        {
            _tcpUrl = await ReplicationUtils.GetTcpInfoAsync(
                _context,
                _destination.Url,
                _destination.Database,
                _destination.ApiKey);

            await ConnectSocketAsync();
            using (var stream = _tcpClient.GetStream())            
            using (var writer = new BlittableJsonTextWriter(_context, stream))
            {
                _context.Write(writer, new DynamicJsonValue
                {
                    [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _destination.Database,
                    [nameof(TcpConnectionHeaderMessage.Operation)] =
                        TcpConnectionHeaderMessage.OperationTypes.TopologyDiscovery.ToString(),
                });

                var alreadyVisitedJson = new DynamicJsonValue();
                foreach (var kvp in _alreadyVisited)
                    alreadyVisitedJson[kvp.Key] = new DynamicJsonArray(kvp.Value);

                _context.Write(writer, new DynamicJsonValue
                {
                    [nameof(TopologyDiscoveryRequest.AlreadyVisited)] = alreadyVisitedJson,
                    [nameof(TopologyDiscoveryRequest.Timeout)] = _timeout,
                    [nameof(TopologyDiscoveryRequest.OriginDbId)] = _dbId
                });

                writer.Flush();

                //now parse the response                
                using (var parser = _context.ParseMultiFrom(stream))
                using (var topologyResponseJson = await parser.ParseToMemoryAsync("ReplicationDiscovere/Read-topology-response"))
                { 
                    topologyResponseJson.BlittableValidation();
                    var topologyResponse = JsonDeserializationServer.TopologyDiscoveryResponse(topologyResponseJson);
                    if (topologyResponse.DiscoveryStatus == TopologyDiscoveryResponse.Status.Ok ||
                        topologyResponse.DiscoveryStatus == TopologyDiscoveryResponse.Status.Leaf)
                    {
                        using (var topologyInfoJson = await parser.ParseToMemoryAsync("ReplicationDiscovere/Read-topology-info"))
                        {
                            topologyInfoJson.BlittableValidation();
                            return JsonDeserializationServer.NodeTopologyInfo(topologyInfoJson);
                        }
                    }

                }

                return null;
            }
        }

        public void Dispose()
        {
            _tcpClient.Dispose();
        }

        private async Task ConnectSocketAsync()
        {
            var uri = new Uri(_tcpUrl);
            var host = uri.Host;
            var port = uri.Port;
            try
            {
                await _tcpClient.ConnectAsync(host, port);
            }
            catch (SocketException e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {_tcpUrl} for topology discovery. Socket Error Code = {e.SocketErrorCode}", e);
                throw;
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Failed to connect to remote replication destination {_tcpUrl}  for topology discovery.", e);
                throw;
            }
        }
    }
}
