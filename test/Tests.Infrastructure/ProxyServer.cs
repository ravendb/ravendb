// -----------------------------------------------------------------------
//  <copyright file="ProxyServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Tests.Infrastructure
{
    public class ProxyServer : IAsyncDisposable
    {
        private readonly int _to;
        private readonly TcpListener _listener;
        private readonly ConcurrentSet<ProxyConnection> _connections = new ConcurrentSet<ProxyConnection>();
        
        public long TotalWrite => _connections.Sum(x => x.TotalWrite);
        public long TotalRead => _connections.Sum(x => x.TotalRead);
        private bool _isRunning;

        public int Port;
        public Func<long, ArraySegment<byte>, bool> VetoTransfer = delegate { return false; };
        public int ConnectionDelay;

        public ProxyServer(ref int port, int to, int delay = 0)
        {
            _to = to;
            var originPort = port;

            ConnectionDelay = delay;

            while (port > 0)
            {
                try
                {
                    _listener = new TcpListener(IPAddress.Loopback, port);
                    _listener.Start();
                    break;
                }
                catch (SocketException)
                {
                    port--;
                    _listener = null;
                }
            }

            if (_listener == null)
                throw new InvalidOperationException("Couldn't find an open port in the range " + port + " - " + originPort);

            Port = port;
            _isRunning = true;
            Task.Run(ListenToConnection);
        }

        private async Task ListenToConnection()
        {
            while (_isRunning)
            {
                var client = await _listener.AcceptTcpClientAsync();
                var connection = new ProxyConnection(this, client, _to);
                _connections.Add(connection);
                connection.Transfer();
            }
        }

        private class ProxyConnection : IAsyncDisposable
        {
            private readonly ProxyServer _parent;
            private readonly TcpClient _source;
            private readonly TcpClient _destination;
            private readonly int _to;

            private long _totalWrite;
            private long _totalRead;

            public long TotalWrite => _totalWrite;
            public long TotalRead => _totalRead;

            
            private Task _task;

            public ProxyConnection(ProxyServer parent, TcpClient source, int to)
            {
                _parent = parent;
                _source = source;
                _to = to;
                _destination = new TcpClient(source.Client.AddressFamily);
            }

            public void Transfer()
            {
                _task = TransferAsync();
            }

            private async Task TransferAsync()
            {
                await _destination.ConnectAsync(IPAddress.Loopback, _to).ConfigureAwait(false);
                await using (var srcStream = _source.GetStream())
                await using (var dstStream = _destination.GetStream())
                {
                    var t1 = Task.Run(() => CopyWithLambda(srcStream, dstStream, ref _totalWrite));
                    var t2 = Task.Run(() => CopyWithLambda(dstStream, srcStream, ref _totalRead));

                    await Task.WhenAll(t1, t2).ConfigureAwait(false);
                }
            }

            private void CopyWithLambda(Stream s, Stream d, ref long total)
            {
                var buffer = new byte[4096];

                try
                {
                    while (true)
                    {
                        if (_parent.ConnectionDelay > 0)
                            Thread.Sleep(_parent.ConnectionDelay);

                        var read = s.Read(buffer, 0, 4096);
                        if (read == 0)
                            break;
                        var tmpTotal = Interlocked.Add(ref total, read);
                        if (_parent.VetoTransfer(tmpTotal, new ArraySegment<byte>(buffer, 0, read)))
                        {
                            //force close of both streams
                            s.Dispose();
                            d.Dispose();
                            throw new Exception("Transfer vetoed!");
                        }

                        d.Write(buffer, 0, read);
                    }
                }
                catch (IOException)
                {
                }
            }

            public async ValueTask DisposeAsync()
            {
                try
                {
                    _source.Dispose();
                }
                catch
                {
                    //
                }

                try
                {
                    _destination.Dispose();
                }
                catch
                {
                    //
                }

                try
                {
                    await _task;
                }
                catch
                {
                    //
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _isRunning = false;
            _listener.Stop();
            foreach (var connection in _connections)
            {
                await connection.DisposeAsync();
            }
        }
    }
}
