using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AsyncFriendlyStackTrace;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Server.Config.Attributes;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server
{
    public class RavenServerStartup
    {
        private RequestRouter _router;
        private RavenServer _server;
        private int _requestId;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("Server");

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
            if (IsServerRuningInASafeManner() == false)
            {
                app.Run(UnsafeRequestHandler);
                return;
            }            
            app.Run(RequestHandler);
        }

        private bool IsServerRuningInASafeManner()
        {
            if (_server.Configuration.Server.AnonymousUserAccessMode == AnonymousUserAccessModeValues.None)
                return true;
            if (_server.Configuration.Server.AllowEverybodyToAccessTheServerAsAdmin)
                return true;
            var url = _server.Configuration.Core.ServerUrl.ToLowerInvariant();
            var uri = new Uri(url);
            //url isn't set to localhost 
            return uri.IsLoopback || uri.Host == "localhost.fiddler";
        }

        public static bool SkipHttpLogging;

        private static readonly HashSet<string> RoutesAllowedInUnsafeMode = new HashSet<string> {
            "/admin/stats/server-id"
        };

        private Task UnsafeRequestHandler(HttpContext context)
        {
            if (RoutesAllowedInUnsafeMode.Contains(context.Request.Path.Value))
            {
                return RequestHandler(context);
            }

            context.Response.StatusCode = (int) HttpStatusCode.ServiceUnavailable;
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            JsonOperationContext ctx;
            using (_server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
            using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Message");
                writer.WriteString(String.Join(" ",UnsafeWarning));
                writer.WriteComma();
                writer.WritePropertyName("MessageAsArray");
                writer.WriteStartArray();
                var first = true;
                foreach (var val in UnsafeWarning)
                {
                    if(first == false)
                        writer.WriteComma();
                    first = false;
                    writer.WriteString(val);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }

        private static readonly string[] UnsafeWarning = {
            "The server is running in a potentially unsafe mode.",
            "This means that Raven/AnonymousUserAccessMode is set to Admin and expose to the world.",
            "Prevent unsafe access to the server by setting Raven/AnonymousUserAccessMode to None.",
            "If you intended to give everybody admin access to the server than set Raven/AllowEverybodyToAccessTheServerAsAdmin to true.",
            "In order to gain access to the server please run in on localhost."
        };

        private async Task RequestHandler(HttpContext context)
        {
            try
            {                
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";

                var sp = Stopwatch.StartNew();
                var tenant = await _router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
                sp.Stop();

                if (_logger.IsInfoEnabled && SkipHttpLogging == false)
                {
                    _logger.Info($"{context.Request.Method} {context.Request.Path.Value}?{context.Request.QueryString.Value} - {context.Response.StatusCode} - {sp.ElapsedMilliseconds:#,#;;0} ms");
                }

                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var requestId = Interlocked.Increment(ref _requestId);

                    var twn = new TrafficWatchChange
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
                //TODO: operaton cancelled (timeout)
                //TODO: Invalid data exception 422


                //TODO: Proper json output, not like this
                var response = context.Response;

                MaybeSetExceptionStatusCode(response, e);
            
                JsonOperationContext ctx;
                using (_server.ServerStore.ContextPool.AllocateOperationContext(out ctx))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = $"{context.Request.Path}{context.Request.QueryString}",
                        [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                        [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message
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

#if EXCEPTION_ERROR_HUNT
                    var f = Guid.NewGuid() + ".error";
                    File.WriteAllText(f,
                        $"{context.Request.Path}{context.Request.QueryString}" + Environment.NewLine + errorString);
#endif
                    djv[nameof(ExceptionDispatcher.ExceptionSchema.Error)] = errorString;

                    MaybeAddAdditionalExceptionData(djv, e);

                    using (var writer = new BlittableJsonTextWriter(ctx, response.Body))
                    {
                        var json = ctx.ReadObject(djv, "exception");
                        writer.WriteObject(json);
                    }

#if EXCEPTION_ERROR_HUNT
                    File.Delete(f);
#endif

                }
            }
        }

        private void MaybeAddAdditionalExceptionData(DynamicJsonValue djv, Exception exception)
        {
            var indexCompilationException = exception as IndexCompilationException;
            if (indexCompilationException != null)
            {
                djv[nameof(IndexCompilationException.IndexDefinitionProperty)] = indexCompilationException.IndexDefinitionProperty;
                djv[nameof(IndexCompilationException.ProblematicText)] = indexCompilationException.ProblematicText;
                return;
            }

            var transformerCompilationException = exception as TransformerCompilationException;
            if (transformerCompilationException != null)
            {
                djv[nameof(TransformerCompilationException.TransformerDefinitionProperty)] = transformerCompilationException.TransformerDefinitionProperty;
                djv[nameof(TransformerCompilationException.ProblematicText)] = transformerCompilationException.ProblematicText;
                return;
            }

            var documentConflictException = exception as DocumentConflictException;
            if (documentConflictException != null)
            {
                djv[nameof(DocumentConflictException.DocId)] = documentConflictException.DocId;
                djv[nameof(DocumentConflictException.LargestEtag)] = documentConflictException.LargestEtag;
            }
        }

        private static void MaybeSetExceptionStatusCode(HttpResponse response, Exception exception)
        {
            if (response.HasStarted)
                return;

            if (exception is DocumentConflictException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is ConflictException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is ConcurrencyException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is DatabaseNotFoundException || exception is DatabaseDisabledException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            if (exception is BadRequestException)
            {
                response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
    }
}
