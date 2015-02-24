// -----------------------------------------------------------------------
//  <copyright file="HttpTransport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Interfaces;
using Rachis.Messages;

namespace Rachis.Transport
{
	public class HttpTransport : ITransport, IDisposable
	{
		private readonly HttpTransportBus _bus;
		private readonly HttpTransportSender _sender;

		public HttpTransport(string name)
		{
			_bus = new HttpTransportBus(name);
			_sender = new HttpTransportSender(name,_bus);
		}

		public void Send(NodeConnectionInfo dest, DisconnectedFromCluster req)
		{
			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, AppendEntriesRequest req)
		{
			_sender.Send(dest, req);
		}

		public void Stream(NodeConnectionInfo dest, InstallSnapshotRequest req, Action<Stream> streamWriter)
		{
			_sender.Stream(dest, req, streamWriter);
		}

		public void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req)
		{
			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, RequestVoteRequest req)
		{
			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, TimeoutNowRequest req)
		{
			_sender.Send(dest, req);
		}

		public void SendToSelf(AppendEntriesResponse resp)
		{
			_bus.SendToSelf(resp);
		}

		public void Publish(object msg, TaskCompletionSource<HttpResponseMessage> source, Stream stream = null)
		{
			_bus.Publish(msg, source, stream);
		}

		public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
		{
			return _bus.TryReceiveMessage(timeout, cancellationToken, out messageContext);
		}

		public void Dispose()
		{
			_bus.Dispose();
			_sender.Dispose();
		}

		public HttpTransportBus Bus
		{
			get { return _bus; }
		}
	}
}