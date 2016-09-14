using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using AsyncFriendlyStackTrace;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Server.Kestrel.Internal.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Config;
using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Exceptions;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using LogManager = NLog.LogManager;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        private RequestRouter _router;
        private RavenServer _server;
        private int _requestId;

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerfactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                // TODO: KeepAlive causes "Unexpect reserved bit set" (we are sending our own hearbeats, so we do not need this)
                //KeepAliveInterval = Debugger.IsAttached ? 
                //    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30), 
                KeepAliveInterval = TimeSpan.FromHours(24),
                ReceiveBufferSize = 4096,
            });

            _router = app.ApplicationServices.GetService<RequestRouter>();
            _server = app.ApplicationServices.GetService<RavenServer>();
            app.Run(RequestHandler);
        }

        private async Task RequestHandler(HttpContext context)
        {
            try
            {
                context.Response.StatusCode = 200;
                var sp = Stopwatch.StartNew();
                var tenant = await _router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
                sp.Stop();

                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var requestId = Interlocked.Increment(ref _requestId);

                    var twn = new TrafficWatchNotification
                    {
                        TimeStamp = DateTime.UtcNow,
                        RequestId = requestId, // counted only for traffic watch
                        HttpMethod = context.Request.Method ?? "N/A", // N/A ?
                        ElapsedMilliseconds = sp.ElapsedMilliseconds,
                        ResponseStatusCode = context.Response.StatusCode,
                        RequestUri = context.Request.GetEncodedUrl(),
                        AbsoluteUri = $@"{context.Request.Scheme}://{context.Request.Host}",
                        TenantName = tenant ?? "N/A",
                        CustomInfo = "", // TODO: Implement
                        InnerRequestsCount = 0, // TODO: Implement
                        QueryTimings = null, // TODO: Implement
                    };

                    TrafficWatchManager.DispatchMessage(twn);
                }
            }
            catch (Exception e)
            {
                if (context.RequestAborted.IsCancellationRequested)
                    return;
                //TODO: special handling for argument exception (400 bad request)
                //TODO: database not found (503)
                //TODO: operaton cancelled (timeout)
                //TODO: Invalid data exception 422


                //TODO: Proper json output, not like this
                var response = context.Response;

                var documentConflictException = e as DocumentConflictException;
                if (documentConflictException != null)
                    response.StatusCode = 409;
                else if (response.HasStarted == false && response.StatusCode < 400)
                    response.StatusCode = 500;

                JsonOperationContext ctx;
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
                {
                    // this should be changed to BlittableJson
                    

                    var djv = new DynamicJsonValue
                    {
                        ["Url"] = $"{context.Request.Path}?{context.Request.QueryString}",
                        ["Type"] = e.GetType().FullName,
                        ["Message"] = e.Message
                    };

                    string errorString;

                    try
                    {
                        errorString = e.ToAsyncString();
                    }
                    catch (Exception)
                    {
                        errorString = e.ToString();
                    }

                    djv["Error"] = errorString;

                    var indexCompilationException = e as IndexCompilationException;
                    if (indexCompilationException != null)
                    {
                        djv[nameof(IndexCompilationException.IndexDefinitionProperty)] =
                            indexCompilationException.IndexDefinitionProperty;
                        djv[nameof(IndexCompilationException.ProblematicText)] =
                            indexCompilationException.ProblematicText;
                    }

                    if (documentConflictException != null)
                    {
                        djv["ConflictInfo"] = ReplicationUtils.GetJsonForConflicts(
                            documentConflictException.DocId,
                            documentConflictException.Conflicts);
                    }

                    using (var writer = new BlittableJsonTextWriter(ctx, response.Body))
                    {
                        var json = ctx.ReadObject(djv, "exception");
                        writer.WriteObject(json);
                    }
                }
            }
        }
    }
}
