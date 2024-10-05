using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Logging;
using Raven.Server.Utils.Features;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public sealed class PgServer : IDisposable
    {
        private static RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<PgServer>();

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

                if (_server.ServerStore.LicenseManager.CanUsePowerBi(withNotification: false, out _))
                {
                    activate = true;
                }
                else if (_server.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true))
                {
                    if (_server.ServerStore.FeatureGuardian.CanUse(Feature.PostgreSql))
                        activate = true;
                    else
                    {
                        if (_logger.IsWarnEnabled)
                            _logger.Warn($"You have enabled the PostgreSQL integration via '{RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)}' configuration but " +
                                         "this is an experimental feature and the server does not support experimental features. " +
                                         $"Please enable experimental features by changing '{RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)}' configuration value to '{nameof(FeaturesAvailability.Experimental)}'.");
                    }
                }

                if (activate)
                {
                    if (Active == false)
                        Start();
                }
                else
                    Stop();
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
                    task.Wait(TimeSpan.FromSeconds(3));
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
                    _server.Certificate,
                    identifier,
                    _processId,
                    _server.ServerStore.DatabasesLandlord,
                    _cts.Token);

                await session.Run();
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Failed to handle Postgres connection (session ID: {identifier})", e);
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
                            if (_logger.IsWarnEnabled)
                                _logger.Warn($"Failed to accept TCP client (port: {((IPEndPoint)tcpListener.LocalEndpoint).Port})", e);

                            continue;
                        }

                        if (client == null)
                            continue;

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
