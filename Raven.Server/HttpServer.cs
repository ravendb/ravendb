using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using Raven.Database.Exceptions;
using Raven.Server.Responders;
using Raven.Database.Extensions;

namespace Raven.Server
{
    public class HttpServer : IDisposable
    {
        private readonly RequestResponder[] requestResponders;
        private readonly HttpListener listener;
        private int reqNum;

        private ILog logger = LogManager.GetLogger(typeof (HttpServer));

        public HttpServer(IEnumerable<RequestResponder> requestResponders)
            : this(8080, requestResponders)
        {

        }

        public HttpServer(
            int port,
            IEnumerable<RequestResponder> requestResponders)
        {
            this.requestResponders = requestResponders.ToArray();
            listener = new HttpListener();
            listener.Prefixes.Add("http://+:" + port + "/");
        }

        public void Start()
        {
            listener.Start();
            listener.BeginGetContext(GetContext, null);
        }

        private void GetContext(IAsyncResult ar)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = listener.EndGetContext(ar);
                //setup waiting for the next request
                listener.BeginGetContext(GetContext, null);
            }
            catch (HttpListenerException)
            {
                // can't get current request / end new one, probably
                // listner shutdown
                return;
            }

            var curReq = Interlocked.Increment(ref reqNum);
            try
            {
                logger.DebugFormat("Request #{0}: {1} {2}",
                                   curReq,
                                   ctx.Request.HttpMethod,
                                   ctx.Request.Url.PathAndQuery
                    );
                var sw = Stopwatch.StartNew();
                HandleRequest(ctx);

                logger.DebugFormat("Request #{0}: {1}",
                                   curReq, sw.Elapsed);
            }
            catch (Exception e)
            {
                HandleException(ctx, e);
                logger.WarnFormat(e, "Error on request #{0}", curReq);
            }
            finally
            {
                try
                {
                    ctx.Response.OutputStream.Flush();
                    ctx.Response.Close();
                }
                catch
                {
                }
            }
        }

        private void HandleException(HttpListenerContext ctx, Exception e)
        {
            try
            {
                if (e is BadRequestException)
                    HandleBadRequest(ctx, (BadRequestException)e);
                else if (e is ConcurrencyException)
                    HandleConcurrencyException(ctx, (ConcurrencyException)e);
                else
                    HandleGenericException(ctx, e);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void HandleGenericException(HttpListenerContext ctx, Exception e)
        {
            ctx.Response.StatusCode = 500;
            ctx.Response.StatusDescription = "Internal Server Error";
            using (var sw = new StreamWriter(ctx.Response.OutputStream))
            {
                new JsonSerializer().Serialize(sw,
                                               new
                                               {
                                                   url = ctx.Request.RawUrl,
                                                   error = e.ToString()
                                               });
            }
        }

        private void HandleBadRequest(HttpListenerContext ctx, BadRequestException e)
        {
            ctx.SetStatusToBadRequest();
            using (var sw = new StreamWriter(ctx.Response.OutputStream))
            {
                new JsonSerializer().Serialize(sw,
                                               new
                                               {
                                                   url = ctx.Request.RawUrl,
                                                   message = e.Message,
                                                   error = e.Message
                                               });
            }
        }

        private void HandleConcurrencyException(HttpListenerContext ctx, ConcurrencyException e)
        {
            ctx.Response.StatusCode = 409;
            ctx.Response.StatusDescription = "Conflict";
            using (var sw = new StreamWriter(ctx.Response.OutputStream))
            {
                new JsonSerializer().Serialize(sw,
                                               new
                                               {
                                                   url = ctx.Request.RawUrl,
                                                   actualETag = e.ActualETag,
                                                   expectedETag = e.ExpectedETag,
                                                   error = e.Message
                                               });
            }
        }

        private void HandleRequest(HttpListenerContext ctx)
        {
            foreach (var requestResponder in requestResponders)
            {
                if (requestResponder.WillRespond(ctx))
                {
                    requestResponder.Respond(ctx);
                    return;
                }
            }
            ctx.SetStatusToNotFound();
            ctx.Write(
                @"
<html>
    <body>
        <h1>Not Found</h1>
        <p>Could not find a matching document on the server</p>
    </body>
</html>
");
        }

        public void Dispose()
        {
            listener.Stop();
        }
    }
}