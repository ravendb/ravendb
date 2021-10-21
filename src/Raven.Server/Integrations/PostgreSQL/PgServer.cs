using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class PgServer : IDisposable
    {
        private readonly RavenServer _server;
        private readonly ConcurrentDictionary<TcpClient, Task> _connections = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);
        private readonly int _processId;
        private readonly int _port;

        private Task _listenTask = Task.CompletedTask;
        private TcpListener _tcpListener;
        private int _sessionIdentifier;
        private bool _disposed;
        
        public PgServer(RavenServer server)
        {
            _server = server;
            _processId = Process.GetCurrentProcess().Id;
            _port = _server.Configuration.Integrations.PostgreSql.Port;

            _server.ServerStore.LicenseManager.LicenseChanged += OnLicenseChanged;
        }

        public bool Active { get; private set; }

        public void Execute()
        {
            if (_server.Configuration.Integrations.PostgreSql.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                var activate = _server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true);
                if (activate)
                {
                    if (Active)
                        throw new InvalidOperationException("Cannot start PgServer because it is already activated. Should not happen!");

                    Start();
                }
            }
            finally
            {
                _locker.Release();
            }
        }

        private void Start()
        {
            _tcpListener = new TcpListener(IPAddress.Any, _port);
            _tcpListener.Start();

            _listenTask = ListenToConnectionsAsync();

            Active = true;
        }

        private void Stop()
        {
            _tcpListener.Stop();
            _cts.Cancel();
            foreach (var (_, task) in _connections)
            {
                task.Wait();
            }

            Active = false;
        }

        public async Task HandleConnection(TcpClient client)
        {
            try
            {
                var session = new Session(
                    client,
                    Interlocked.Increment(ref _sessionIdentifier),
                    _processId,
                    _server.ServerStore.DatabasesLandlord,
                    _server.Certificate.Certificate != null,
                    _server.AuthenticateAsServerIfSslNeeded,
                    _cts.Token);

                await session.Run();
            }
            catch (Exception e)
            {
                // TODO: Error handling (won't be needed after integration as Raven.Server has this logic already)
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private async Task ListenToConnectionsAsync()
        {
            while (_cts.IsCancellationRequested == false)
            {
                TcpClient client;
                try
                {
                    client = await _tcpListener.AcceptTcpClientAsync();
                }
                catch (Exception e)
                {
                    // TODO: Error handling (won't be needed after integration as Raven.Server has this logic already)
                    throw;
                }

                _connections.TryAdd(client, HandleConnection(client));
            }
        }

        internal int GetListenerPort()
        {
            int port = ((IPEndPoint)_tcpListener.LocalEndpoint).Port;

            return port;
        }

        private void OnLicenseChanged()
        {
            if (_server.Configuration.Integrations.PostgreSql.Enabled == false)
                return;

            _locker.Wait();

            try
            {
                var activate = _server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true);
                if (activate)
                {
                    if (Active == false)
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

        private void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                _tcpListener.Stop();
                _cts.Cancel();
                foreach (var (_, task) in _connections)
                {
                    task.Wait();
                }
            }

            _disposed = true;
        }
    }
}
