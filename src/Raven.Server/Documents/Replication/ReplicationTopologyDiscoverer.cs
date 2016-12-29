using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Json;
using Raven.NewClient.Replication.Messages;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationTopologyDiscoverer : IDisposable
    {
        private readonly CancellationToken _token;
        private readonly SingleDestinationDiscoverer[] discoverers;

        public int DiscovererCount => discoverers.Length;

        public ReplicationTopologyDiscoverer(
            IEnumerable<OutgoingReplicationHandler> outgoing,
            List<string> alreadyKnownDestinations,
            CancellationToken token)
        {
            _token = token;
            discoverers =
                outgoing.Where(outgoingHandler => !alreadyKnownDestinations.Contains(outgoingHandler.DestinationDbId))                                    
                        .Select(
                        outgoingHandler => 
                          new SingleDestinationDiscoverer(
                            alreadyKnownDestinations,
                            outgoingHandler,
                            new OperationCredentials(outgoingHandler.Destination.ApiKey, CredentialCache.DefaultCredentials),
                            token))                        
                        .ToArray();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async Task<TopologyNode[]> DiscoverTopologyAsync()
        {
            if (discoverers.Length == 0) //either no destinations or we already visited all destinations
                return new TopologyNode[0];

            var discoveryTasks = new List<Task<TopologyNode>>(discoverers.Length);
            foreach(var d in discoverers)
                discoveryTasks.Add(d.DiscoverTopologyAsync());

            return await Task.WhenAll(discoveryTasks);
        }

        public void Dispose()
        {
            foreach (var discoverer in discoverers)
                discoverer.Dispose();
        }

        private class SingleDestinationDiscoverer : IDisposable
        {
            private readonly IEnumerable<string> _alreadyKnownDestinations;
            private readonly OutgoingReplicationHandler _outgoigHandler;
            private readonly OperationCredentials _operationCredentials;
            private readonly CancellationToken _token;
            private TcpConnectionInfo _connectionInfo;
            private readonly TcpClient _tcpClient;
            private readonly Logger _log;

            public SingleDestinationDiscoverer(
                IEnumerable<string> alreadyKnownDestinations, 
                OutgoingReplicationHandler outgoigHandler,
                OperationCredentials operationCredentials,
                CancellationToken token)
            {
                _alreadyKnownDestinations = alreadyKnownDestinations;
                _outgoigHandler = outgoigHandler;
                _operationCredentials = operationCredentials;
                _token = token;
                _log = LoggingSource.Instance.GetLogger<SingleDestinationDiscoverer>(outgoigHandler._database.Name);
                _tcpClient = new TcpClient();
            }

            public async Task<TopologyNode> DiscoverTopologyAsync()
            {
                _connectionInfo = ReplicationUtils.GetTcpInfo(_outgoigHandler.Destination.Url, _operationCredentials);
                await ConnectSocketAsync();
                JsonOperationContext context;
                using (var stream = _tcpClient.GetStream())
                using (_outgoigHandler._database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _outgoigHandler.Destination.Database,
                        [nameof(TcpConnectionHeaderMessage.Operation)] =
                        TcpConnectionHeaderMessage.OperationTypes.TopologyDiscovery.ToString(),
                    });
                    writer.Flush();

                    var alreadyVisitedDbIds =
                        _alreadyKnownDestinations.Union(new[] {_outgoigHandler._database.DbId.ToString()});
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TopologyDiscoveryHeader.AlreadyVisitedDbIds)] = new DynamicJsonArray(alreadyVisitedDbIds),
                    });

                    writer.Flush();

                    TopologyDiscoveryResponse response;
                    using (var parser = context.ParseMultiFrom(stream))
                    using (var responseJson = await parser.ParseToMemoryAsync("ReplicationDiscovere/Read-discovery-response"))
                        response = JsonManualDeserialization.ConvertToDiscoveryResponse(responseJson);

                    switch (response.DiscoveryResponseType)
                    {
                        case TopologyDiscoveryResponse.ResponseType.Ok:
                            return response.DiscoveredTopology;
                        //TODO : do proper error handling
                        case TopologyDiscoveryResponse.ResponseType.Error:
                            throw new InvalidOperationException();
                        case TopologyDiscoveryResponse.ResponseType.Timeout:
                            throw new TimeoutException();
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

         

            public void Dispose()
            {
                _tcpClient.Dispose();
            }

            private async Task ConnectSocketAsync()
            {
                var uri = new Uri(_connectionInfo.Url);
                var host = uri.Host;
                var port = uri.Port;
                try
                {
                    await _tcpClient.ConnectAsync(host, port);
                }
                catch (SocketException e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed to connect to remote replication destination {_connectionInfo.Url} for topology discovery. Socket Error Code = {e.SocketErrorCode}", e);
                    throw;
                }
                catch (Exception e)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info($"Failed to connect to remote replication destination {_connectionInfo.Url}  for topology discovery.", e);
                    throw;
                }
            }         
        }
    }    
}
