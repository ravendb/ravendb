using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Config;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
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
                // TODO: KeepAlive causes "Unexpect reserved bit set" (we are sending our own heartbeats, so we do not need this)
                //KeepAliveInterval = Debugger.IsAttached ? 
                //    TimeSpan.FromHours(24) : TimeSpan.FromSeconds(30), 
                KeepAliveInterval = TimeSpan.FromHours(24),
                ReceiveBufferSize = 4096
            });

            _router = app.ApplicationServices.GetService<RequestRouter>();
            _server = app.ApplicationServices.GetService<RavenServer>();

            if (_server.Configuration.Http.UseResponseCompression)
            {
                // Enable automatic response compression for all routes that
                // are not studio's statics. The studio takes care of its own
                // compression.
                app.UseWhen(
                    context => context.Request.Path.StartsWithSegments("/studio") == false,
                    appBuilder => appBuilder.UseResponseCompression());
            }


            if (IsServerRunningInASafeManner() == false)
            {
                app.Use(_ => UnsafeRequestHandler);
                return;
            }

            app.Use(_ => RequestHandler);
        }

        private bool IsServerRunningInASafeManner()
        {
            if (_server.Configuration.Security.AuthenticationEnabled)
                return true;

            return _server.Configuration.Security.IsUnsecureAccessSetupValid ?? false;
        }

        public static bool SkipHttpLogging;

        private static readonly HashSet<string> RoutesAllowedInUnsafeMode = new HashSet<string> {
            "/debug/server-id"
        };

        private Task UnsafeRequestHandler(HttpContext context)
        {
            if (RoutesAllowedInUnsafeMode.Contains(context.Request.Path.Value))
            {
                return RequestHandler(context);
            }

            context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;

            if (IsHtmlAcceptable(context))
            {
                context.Response.Headers["Content-Type"] = "text/html; charset=utf-8";
                return context.Response.WriteAsync(HtmlUtil.RenderUnsafePage());
            }

            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Message");
                writer.WriteString(string.Join(" ", UnsafeWarning));
                writer.WriteComma();
                writer.WritePropertyName("MessageAsArray");
                writer.WriteStartArray();
                var first = true;
                foreach (var val in UnsafeWarning)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    writer.WriteString(val);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        public static bool IsHtmlAcceptable(HttpContext context)
        {
            bool result = false;
            var acceptHeaders = context.Request.Headers["Accept"].ToArray();
            foreach (var acceptHeader in acceptHeaders)
            {
                if (acceptHeader != null
                    && (acceptHeader.Contains("text/html")
                        || acceptHeader.Contains("text/*")))
                {
                    result = true;
                }
            }

            return result;
        }

        private static readonly string[] UnsafeWarning = {
            "Running in a potentially unsafe mode.",
            "Server certificate information has not been set up and the server address is not configured within allowed unsecured access address range.",
            $"Please find the RavenDB settings file *settings.json* in the server directory and fill in your certificate information in either { RavenConfiguration.GetKey(x => x.Security.CertificatePath) } or { RavenConfiguration.GetKey(x => x.Security.CertificateExec) }",
            $"If you would rather like to keep your server unsecured, please relax the { RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed) } setting to match the { RavenConfiguration.GetKey(x => x.Core.ServerUrl) } setting value."
        };

        private async Task RequestHandler(HttpContext context)
        {
            try
            {
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";

                var sp = Stopwatch.StartNew();
                var database = await _router.HandlePath(context, context.Request.Method, context.Request.Path.Value);
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
                        DatabaseName = database ?? "N/A",
                        CustomInfo = "", // TODO: Implement
                        InnerRequestsCount = 0, // TODO: Implement
                        QueryTimings = null // TODO: Implement
                    };

                    TrafficWatchManager.DispatchMessage(twn);
                }
            }
            catch (Exception e)
            {
                if (context.RequestAborted.IsCancellationRequested)
                    return;

                //TODO: special handling for argument exception (400 bad request)
                //TODO: operation canceled (timeout)
                //TODO: Invalid data exception 422


                //TODO: Proper json output, not like this
                var response = context.Response;

                MaybeSetExceptionStatusCode(response, e);

                using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = $"{context.Request.Path}{context.Request.QueryString}",
                        [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                        [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message
                    };


#if EXCEPTION_ERROR_HUNT
                    var f = Guid.NewGuid() + ".error";
                    File.WriteAllText(f,
                        $"{context.Request.Path}{context.Request.QueryString}" + Environment.NewLine + errorString);
#endif
                    djv[nameof(ExceptionDispatcher.ExceptionSchema.Error)] = e.ToString();

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

            if (exception is LowMemoryException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

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

            if (exception is DatabaseDisabledException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            if (exception is BadRequestException)
            {
                response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            if (exception is UnauthorizedAccessException)
            {
                response.StatusCode = (int)HttpStatusCode.Forbidden;
                return;
            }

            if (exception is DatabaseNotRelevantException)
            {
                response.StatusCode = (int)HttpStatusCode.Gone;
                return;
            }

            response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
    }
}
