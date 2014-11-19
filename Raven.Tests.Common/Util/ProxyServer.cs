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

namespace Raven.Tests.Common.Util
{
    public class ProxyServer : IDisposable
    {
        private readonly int to;
        private readonly TcpListener listener;

        public Func<long, ArraySegment<byte>, bool> VetoTransfer = delegate { return true; };
        private long totalRead;
        private long totalWrite;
        private bool isRunning;

        public ProxyServer(ref int port, int to)
        {
            this.to = to;
	        while (port > 0)
	        {
		        try
		        {
					listener = new TcpListener(IPAddress.Loopback, port);
					listener.Start();
			        break;
		        }
		        catch (SocketException)
		        {
			        port--;
		        }
	        }
            totalRead = 0;
            totalWrite = 0;
            isRunning = true;
#pragma warning disable 4014
            ListenToConnection();
#pragma warning restore 4014
        }

        private async Task ListenToConnection()
        {
            while (isRunning)
            {
                var client = await listener.AcceptTcpClientAsync();
#pragma warning disable 4014
                Task.Run(() => AcceptRequests(client));
#pragma warning restore 4014
            }
        }

        private void AcceptRequests(TcpClient src)
        {
            using (src)
            using (var dst = new TcpClient())
            {
                dst.Connect(IPAddress.Loopback, to);
                using (var srcStream = src.GetStream())
                using (var dstStream = dst.GetStream())
                {
                    var t1 = Task.Factory.StartNew(() => CopyWithLambda(srcStream, dstStream, ref totalWrite));
                    var t2 = Task.Factory.StartNew(() => CopyWithLambda(dstStream, srcStream, ref totalRead));
                    Task.WaitAll(t1, t2);
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
                    var read = s.Read(buffer, 0, 4096);
                    if (read == 0)
                        break;
                    var tmpTotal = Interlocked.Add(ref total, read);
					if (VetoTransfer(tmpTotal, new ArraySegment<byte>(buffer, 0, read)))
                    {
                        //force close of both streams
                        s.Close();
                        d.Close();
                        throw new Exception("Transfer vetoed!");
                    }

                    d.Write(buffer, 0, read);
                    if (IsEndOfChunkEncoding(buffer, 0, read))
                    {
                        break;
                    }
                }
            }
            catch (IOException)
            {
            }

        }

        private static bool IsEndOfChunkEncoding(byte[] buffer, int offset, int count)
        {
            // end of chunk encoding:
            // 0x30, 0x0d, 0x0a, 0x0d, 0x0a
            // 0     \r    \n    \r    \n

            var expectedEnd = new byte[] { 0x30, 0x0d, 0x0a, 0x0d, 0x0a };
            
            if (count < expectedEnd.Length)
            {
                return false;
            }

            var bufferStartPos = offset + count - expectedEnd.Length;
            for (var i = 0; i < expectedEnd.Length; i++)
            {
                if (buffer[bufferStartPos + i] != expectedEnd[i])
                {
                    return false;
                }
            }

            return true;
        }

        public void Dispose()
        {
            isRunning = false;
            listener.Stop();
        }
    }
}