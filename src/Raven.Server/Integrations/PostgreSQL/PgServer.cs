using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class PgServer : IDisposable
    {
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<PgServer>("Postgres Server");

        private readonly RavenServer _server;
        private readonly ConcurrentDictionary<TcpClient, Task> _connections = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);
        private readonly ConcurrentQueue<TcpListener> _listeners = new ConcurrentQueue<TcpListener>();
        private readonly int _processId;
        private readonly int _port;

        private int _sessionIdentifier;
        private bool _disposed;
        
        public PgServer(RavenServer server)
        {
            _server = server;
            _processId = Process.GetCurrentProcess().Id;
            _port = _server.Configuration.Integrations.PostgreSql.Port;

            _server.ServerStore.LicenseManager.LicenseChanged += HandleServerActivation;
        }

        public bool Active { get; private set; }

        public void Execute()
        {
            HandleServerActivation();
        }

        private void HandleServerActivation()
        {
            if (_server.Configuration.Integrations.PostgreSql.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                bool activate = false;

                if (_server.ServerStore.LicenseManager.CanUsePowerBi(withNotification: false))
                {
                    // TODO are - check is once again with notification enabled on first powerbi query
                    activate = true;
                }
                else if (_server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: false))
                {
                    // TODO arek - assert experimental feature

                    activate = true;
                }

                if (activate)
                {
                    if (Active)
                        throw new InvalidOperationException("Cannot start PgServer because it is already activated. Should not happen!");

                    Start();
                }
                else
                {
                    Stop();
                }
            }
            catch (ObjectDisposedException)
            {
            }
            finally
            {
                _locker.Release();
            }
        }

        private void Start()
        {
            _server.StartTcpListener(ListenToConnections, _port);

            Active = true;
        }

        private void Stop()
        {
            if (Active == false)
                return;

            _cts.Cancel();

            foreach (var tcpListener in _listeners)
            {
                tcpListener.Stop();
            }
            
            foreach (var (_, task) in _connections)
            {
                try
                {
                    task.Wait();
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }

            Active = false;
        }

        public async Task HandleConnection(TcpClient client)
        {
            int identifier = Interlocked.Increment(ref _sessionIdentifier);

            try
            {
                var session = new PgSession(
                    client,
                    _server.Certificate.Certificate,
                    identifier,
                    _processId,
                    _server.ServerStore.DatabasesLandlord,
                    _cts.Token);

                await session.Run();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to handle Postgres connection (session ID: {identifier})", e);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void ListenToConnections(TcpListener tcpListener)
        {
            Task.Factory.StartNew(async () =>
            {
                _listeners.Enqueue(tcpListener);

                TcpClient client = null;

                try
                {
                    while (_cts.IsCancellationRequested == false)
                    {
                        try
                        {
                            client = await tcpListener.AcceptTcpClientAsync();
                        }
                        catch (Exception e)
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"Failed to accept TCP client (port: {((IPEndPoint)tcpListener.LocalEndpoint).Port})", e);
                        }

                        _connections.TryAdd(client, HandleConnection(client));
                    }
                }
                finally
                {
                    client?.Dispose();
                }
            });
        }

        internal int GetListenerPort()
        {
            if (_listeners.Count == 0)
                throw new InvalidOperationException("There is no TCP listener");
           
            int port = ((IPEndPoint)_listeners.First().LocalEndpoint).Port;

            return port;
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing) 
                Stop();

            _disposed = true;
        }
    }
}
