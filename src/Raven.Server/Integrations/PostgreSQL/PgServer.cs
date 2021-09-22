using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class PgServer
    {
        private readonly RavenServer _server;
        private readonly ConcurrentDictionary<TcpClient, Task> _connections = new();
        private Task _listenTask = Task.CompletedTask;
        private readonly CancellationTokenSource _cts = new();
        private TcpListener _tcpListener;
        private int _sessionIdentifier;
        private readonly int _processId;
        private readonly int _port;

        public PgServer(RavenServer server)
        {
            _server = server;
            _processId = Process.GetCurrentProcess().Id;
            _port = _server.Configuration.Integrations.PostgreSQL.Port;
        }

        public void Execute()
        {
            _tcpListener = new TcpListener(IPAddress.Any, _port);
            _tcpListener.Start();

            _listenTask = ListenToConnectionsAsync();
        }

        public void Shutdown()
        {
            _tcpListener.Stop();
            _cts.Cancel();
            foreach (var (_, task) in _connections)
            {
                task.Wait();
            }
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

        public async Task HandleConnection(TcpClient client)
        {
            try
            {
                var session = new Session(
                    client,
                    _server.AuthenticateAsServerIfSslNeeded,
                    Interlocked.Increment(ref _sessionIdentifier),
                    _processId,
                    _cts.Token);

                await session.Run();
            }
            catch (Exception e)
            {
                // TODO: Error handling (won't be needed after integration as Raven.Server has this logic already)
            }
        }
    }
}
