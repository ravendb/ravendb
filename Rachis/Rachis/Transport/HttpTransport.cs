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
	public class HttpTransport : ITransport
	{
		private readonly CancellationToken _cancellationToken;

		private readonly HttpTransportBus _bus;
		private readonly HttpTransportSender _sender;

		public HttpTransport(string name, CancellationToken cancellationToken)
		{
			_cancellationToken = cancellationToken;

			_bus = new HttpTransportBus(name);
			_sender = new HttpTransportSender(name, _bus, cancellationToken);
		}

		public void Send(NodeConnectionInfo dest, DisconnectedFromCluster req)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, AppendEntriesRequest req)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Send(dest, req);
		}

		public void Stream(NodeConnectionInfo dest, InstallSnapshotRequest req, Action<Stream> streamWriter)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Stream(dest, req, streamWriter);
		}

		public void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, RequestVoteRequest req)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Send(dest, req);
		}

		public void Send(NodeConnectionInfo dest, TimeoutNowRequest req)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_sender.Send(dest, req);
		}

		public void SendToSelf(AppendEntriesResponse resp)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

			_bus.SendToSelf(resp);
		}

		public void Publish(object msg, TaskCompletionSource<HttpResponseMessage> source, Stream stream = null)
		{
			if (_cancellationToken.IsCancellationRequested)
				return;

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