// -----------------------------------------------------------------------
//  <copyright file="ProxyServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public class ProxyServer : IDisposable
    {
        private readonly int _to;
        private readonly TcpListener _listener;

        public Func<long, ArraySegment<byte>, bool> VetoTransfer = delegate { return false; };
        public int ConnectionDelay;
        private long _totalRead;
        private long _totalWrite;
        private bool _isRunning;

        public int Port;

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
            _totalRead = 0;
            _totalWrite = 0;
            _isRunning = true;
#pragma warning disable 4014
            ListenToConnection();
#pragma warning restore 4014
        }

        public long TotalWrite => _totalWrite;
        public long TotalRead => _totalRead;

        private async Task ListenToConnection()
        {
            while (_isRunning)
            {
                var client = await _listener.AcceptTcpClientAsync();
#pragma warning disable 4014
                AcceptRequests(client);
#pragma warning restore 4014
            }
        }

        private async Task AcceptRequests(TcpClient src)
        {
            using (src)
            using (var dst = new TcpClient(src.Client.AddressFamily))
            {
                await dst.ConnectAsync(IPAddress.Loopback, _to).ConfigureAwait(false);
                using (var srcStream = src.GetStream())
                using (var dstStream = dst.GetStream())
                {
                    var t1 = Task.Factory.StartNew(() => CopyWithLambda(srcStream, dstStream, ref _totalWrite));
                    var t2 = Task.Factory.StartNew(() => CopyWithLambda(dstStream, srcStream, ref _totalRead));
                    await Task.WhenAll(t1, t2).ConfigureAwait(false);
                }
            }
        }

        private void CopyWithLambda(Stream s, Stream d, ref long total)
        {
            var buffer = new byte[4096];

            try
            {
                while (true)
                {
                    if (ConnectionDelay > 0)
                        Thread.Sleep(ConnectionDelay);

                    var read = s.Read(buffer, 0, 4096);
                    if (read == 0)
                        break;
                    var tmpTotal = Interlocked.Add(ref total, read);
                    if (VetoTransfer(tmpTotal, new ArraySegment<byte>(buffer, 0, read)))
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

        public void Dispose()
        {
            _isRunning = false;
            _listener.Stop();
        }
    }
}
