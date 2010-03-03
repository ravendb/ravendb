using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Newtonsoft.Json;
using Raven.Database.Exceptions;
using Raven.Server.Responders;

namespace Raven.Server
{
    public class HttpServer : IDisposable
    {
        private readonly RequestResponder[] requestResponders;
        private readonly HttpListener listener;
        private int reqNum;

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

            try
            {
                var curReq = Interlocked.Increment(ref reqNum);
                Console.WriteLine("Request {0}: {1} {2}",
                                  curReq,
                                  ctx.Request.HttpMethod,
                                  ctx.Request.Url.PathAndQuery
                    );
                var sw = Stopwatch.StartNew();
                HandleRequest(ctx);

                Console.WriteLine("Request {0}: {1}",
                                  curReq, sw.Elapsed);
            }
            catch (ConcurrencyException e)
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
            catch (Exception e)
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
            finally
            {
                try
                {
                    ctx.Response.OutputStream.Flush();
                    ctx.Response.Close();
                }
                catch (Exception)
                {
                }
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