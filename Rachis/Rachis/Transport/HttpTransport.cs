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
        private readonly CancellationTokenSource _linkedTokenSource;

        private bool disposed;

        private readonly HttpTransportBus _bus;
        private readonly HttpTransportSender _sender;

        public HttpTransport(string name, TimeSpan shortOperationsTimeout, CancellationToken parentToken)
        {
            _linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(parentToken);

            _bus = new HttpTransportBus(name);
            _sender = new HttpTransportSender(name, shortOperationsTimeout, _bus, _linkedTokenSource.Token);
        }

        public void Send(NodeConnectionInfo dest, DisconnectedFromCluster req)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Send(dest, req);
        }

        public void Send(NodeConnectionInfo dest, AppendEntriesRequest req)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Send(dest, req);
        }

        public void Stream(NodeConnectionInfo dest, InstallSnapshotRequest req, Action<Stream> streamWriter)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Stream(dest, req, streamWriter);
        }

        public void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Send(dest, req);
        }

        public void Send(NodeConnectionInfo dest, RequestVoteRequest req)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Send(dest, req);
        }

        public void Send(NodeConnectionInfo dest, TimeoutNowRequest req)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _sender.Send(dest, req);
        }

        public void SendToSelf(AppendEntriesResponse resp)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _bus.SendToSelf(resp);
        }

        public void Publish(object msg, TaskCompletionSource<HttpResponseMessage> source, Stream stream = null)
        {
            if (_linkedTokenSource.IsCancellationRequested)
                return;

            _bus.Publish(msg, source, stream);
        }

        public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
        {
            return _bus.TryReceiveMessage(timeout, cancellationToken, out messageContext);
        }

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;

            _linkedTokenSource.Cancel();
            _bus.Dispose();
            _sender.Dispose();
            _linkedTokenSource.Dispose();
        }

        public HttpTransportBus Bus
        {
            get { return _bus; }
        }
    }
}
