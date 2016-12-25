using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.NewClient.Client.Http;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationTopologyDiscoverer : IDisposable
    {
        private readonly CancellationToken _token;
        private readonly SingleDestinationDiscoverer[] discoverers;        

        public ReplicationTopologyDiscoverer(
            IEnumerable<OutgoingReplicationHandler> outgoing, 
            CancellationToken token,
            List<Guid> alreadyKnownDestinations = null)
        {
            _token = token;
            discoverers =
                outgoing.Select(
                        @out => 
                          new SingleDestinationDiscoverer(
                            alreadyKnownDestinations,
                            @out.Destination.Database,
                            MultiDatabase.GetRootDatabaseUrl(@out.Destination.Url),
                            new OperationCredentials(@out.Destination.ApiKey, CredentialCache.DefaultCredentials),
                            token))
                        .ToArray();
        }

        public void StartDiscovery()
        {
            foreach(var discoverer in discoverers)
                discoverer.Start();          
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TopologyNode[]> WaitForTopologyDiscovery()
        {
            return Task.WhenAll(discoverers.Select(d => d.WaitForDiscovery()));
        }

        public void Dispose()
        {
            foreach (var discoverer in discoverers)
                discoverer.Dispose();
        }

        private class SingleDestinationDiscoverer : IDisposable
        {
            private readonly List<Guid> _alreadyKnownDestinations;
            private readonly string _databaseName;
            private readonly CancellationToken _token;
            private readonly TcpConnectionInfo _connectionInfo;
            private readonly TcpClient _tcpClient;
            private readonly Logger _log;

            private readonly Thread _discoveryThread;

            private readonly TaskCompletionSource<TopologyNode> _tcs;

            public SingleDestinationDiscoverer(
                List<Guid> alreadyKnownDestinations,
                string databaseName, 
                string databaseUrl, 
                OperationCredentials operationCredentials,
                CancellationToken token)
            {
                _alreadyKnownDestinations = alreadyKnownDestinations;
                _databaseName = databaseName;
                _token = token;
                _log = LoggingSource.Instance.GetLogger<SingleDestinationDiscoverer>(databaseName);
                _connectionInfo = ReplicationUtils.GetTcpInfo(databaseUrl, operationCredentials);
                _tcpClient = new TcpClient();
                _tcs = new TaskCompletionSource<TopologyNode>();
                _discoveryThread = new Thread(PropagateDiscoveryAndWaitForResponse)
                {
                    IsBackground = true,
                    Name = $"Topology discovery thread, connected to {_connectionInfo.Url}, databaseName={_databaseName}"
                };
            }

            public void Start()
            {
                ConnectSocket();
                _discoveryThread.Start();                
            }

            private void PropagateDiscoveryAndWaitForResponse()
            {
                while (true)
                {
                    if (_token.IsCancellationRequested)
                    {
                        _tcs.TrySetCanceled(_token);
                        break;
                    }



                    Thread.Sleep(500);
                }
            }

            public void Dispose()
            {
                _tcpClient.Dispose();
                _discoveryThread.Join(5000);
            }

            private void ConnectSocket()
            {
                var uri = new Uri(_connectionInfo.Url);
                var host = uri.Host;
                var port = uri.Port;
                try
                {
                    _tcpClient.ConnectAsync(host, port).Wait(_token);
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

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task<TopologyNode> WaitForDiscovery()
            {
                return _tcs.Task;
            }
        }
    }  
}
