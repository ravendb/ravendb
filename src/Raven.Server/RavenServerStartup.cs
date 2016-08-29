using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Xml;
using AsyncFriendlyStackTrace;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Config;
using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using LogManager = NLog.LogManager;

namespace Raven.Server
{
    public class RavenServerStartup
    {

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

            var router = app.ApplicationServices.GetService<RequestRouter>();
            var server = app.ApplicationServices.GetService<RavenServer>();
            app.Run(async context =>
            {
                try
                {
                    var sp = Stopwatch.StartNew();
                    var tenant = await router.HandlePath(context, context.Request.Method, context.Request.Path.Value);

                    var twn = new TrafficWatchNotification
                    {
                        TimeStamp = DateTime.UtcNow,
                        RequestId = 0, // TODO ?
                        HttpMethod = context.Request.Method,
                        ElapsedMilliseconds = sp.ElapsedMilliseconds,
                        ResponseStatusCode = context.Response.StatusCode,
                        RequestUri = "uri", // TODO ?
                        AbsoluteUri = "uri", // TODO ?
                        TenantName = tenant,
                        CustomInfo = "custom info", // TODO ?
                        InnerRequestsCount = 0, // TODO ?
                        QueryTimings = null, // TODO ?
                    };

                    TrafficWatchManager.DispatchMessage(twn);
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

                    if (response.HasStarted == false)
                        response.StatusCode = 500;

                    JsonOperationContext ctx;
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
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
                            djv[nameof(IndexCompilationException.IndexDefinitionProperty)] = indexCompilationException.IndexDefinitionProperty;
                            djv[nameof(IndexCompilationException.ProblematicText)] = indexCompilationException.ProblematicText;
                        }

                        using (var writer = new BlittableJsonTextWriter(ctx, response.Body))
                        {
                            var json = ctx.ReadObject(djv, "exception");
                            writer.WriteObject(json);
                        }
                    }
                }
            });
        }
    }
}