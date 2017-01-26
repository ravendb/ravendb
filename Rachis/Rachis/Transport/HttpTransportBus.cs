// -----------------------------------------------------------------------
//  <copyright file="HttpTransportBus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;

using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Threading;
using System.Threading.Tasks;

using Rachis.Messages;

using Raven.Abstractions.Logging;

namespace Rachis.Transport
{
    public class HttpTransportBus : IDisposable
    {
        private readonly string _name;
        private readonly BlockingCollection<HttpTransportMessageContext> _queue = new BlockingCollection<HttpTransportMessageContext>();

        public HttpTransportBus(string name)
        {
            _name = name;
            Log = LogManager.GetLogger(GetType().Name +"."+ name);
        }

        public ILog Log { get; private set; }

        public bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext)
        {
            if (timeout < 0)
                timeout = 0;

            HttpTransportMessageContext item;
            if (_queue.TryTake(out item, timeout, cancellationToken) == false)
            {
                messageContext = null;
                return false;
            }
            messageContext = item;
            return true;
        }

        private class HttpTransportMessageContext : MessageContext
        {
            private readonly TaskCompletionSource<HttpResponseMessage> _tcs;
            private readonly HttpTransportBus _parent;
            private bool sent;
            public HttpTransportMessageContext(TaskCompletionSource<HttpResponseMessage> tcs, HttpTransportBus parent)
            {
                _tcs = tcs;
                sent = tcs == null;
                _parent = parent;
            }

            private void Reply(bool success, object msg)
            {
                if (_tcs == null)
                    return;


                var httpResponseMessage = new HttpResponseMessage(
                    success ? HttpStatusCode.OK : HttpStatusCode.NotAcceptable
                    );
                if (msg != null)
                {
                    httpResponseMessage.Content = new ObjectContent(msg.GetType(), msg, new JsonMediaTypeFormatter());
                }
                sent = true;
                _tcs.TrySetResult(httpResponseMessage);
            }

            public override void Reply(CanInstallSnapshotResponse resp)
            {
                Reply(resp.Success, resp);	
            }

            public override void Reply(InstallSnapshotResponse resp)
            {
                Reply(resp.Success, resp);	
            }

            public override void Reply(AppendEntriesResponse resp)
            {
                Reply(resp.Success, resp);	
            }

            public override void Reply(RequestVoteResponse resp)
            {
                Reply(resp.VoteGranted, resp);	
            }

            public override void ExecuteInEventLoop(Action action)
            {
                _parent.Publish(action, null);
            }

            public override void Done()
            {
                if (sent)
                    return;// nothing to do

                var httpResponseMessage = new HttpResponseMessage(HttpStatusCode.OK);
                _tcs?.TrySetResult(httpResponseMessage);
            }

            public override void Error(Exception exception)
            {
                _tcs?.TrySetException(exception);
            }
        }

        public void SendToSelf(AppendEntriesResponse resp)
        {
            Publish(resp, null);
        }

        public void Publish(object msg, TaskCompletionSource<HttpResponseMessage> source, Stream stream = null)
        {
            if (msg == null) throw new ArgumentNullException("msg");
            _queue.Add(new HttpTransportMessageContext(source, this)
            {
                Message = msg,
                Stream = stream,
            });
        }

        public void Dispose()
        {

        }
    }
}
