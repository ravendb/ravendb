// -----------------------------------------------------------------------
//  <copyright file="ProxyServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Storage.Voron.Impl;
using Raven.Tests.Bugs.MultiMap;
using Rhino.Mocks.Constraints;

namespace Raven.Tests.Util
{
	public class ProxyServer : IDisposable
	{
		private readonly int to;
		private readonly TcpListener listener;

		public Func<long, ArraySegment<byte>, bool> VetoTransfer = delegate { return true; };
		private long totalRead;
		private bool isRunning;

		public ProxyServer(int from, int to)
		{
			this.to = to;
			listener = new TcpListener(IPAddress.Loopback, @from);
			listener.Start();
			totalRead = 0;
			isRunning = true;
			ListenToConnection();
		}

		private void ListenToConnection()
		{
			listener.AcceptTcpClientAsync()
					.ContinueWith(task =>
					{
						if (isRunning)
						{
							ListenToConnection();
							AcceptRequests(task.Result);
						}
						else
						{
							throw new TimeoutException("The test proxy stopped receiving requests --> VetoTransfer() returned true or the proxy was disposed");
						}
					});
		}

		private void AcceptRequests(TcpClient src)
		{
			using (src)
			using (var dst = new TcpClient())
			{
				dst.Connect(IPAddress.Loopback, to);
				using(var srcStream = src.GetStream())
				using (var dstStream = dst.GetStream())
				{
					var t1 = Task.Factory.StartNew(() => CopyWithLambda(srcStream, dstStream));
					var t2 = Task.Factory.StartNew(() => CopyWithLambda(dstStream, srcStream));
					Task.WaitAll(t1,t2);
					
					srcStream.Close();
					dstStream.Close();
				}
				dst.Close();
			}
		}

		private void CopyWithLambda(Stream s, Stream d)
		{
			var buffer = new byte[1024];
			try
			{
				while (true)
				{
					var read = s.Read(buffer, 0, 1024);
					if (read == 0)
						break;
					Interlocked.Add(ref totalRead, read);
					if (VetoTransfer(totalRead, new ArraySegment<byte>(buffer, 0, read)) == false)
					{
						Dispose();
						break;
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
			isRunning = false;
			listener.Stop();
		}
	}
}