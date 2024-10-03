using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Routing;
using Raven.Client.Exceptions.Security;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Exceptions;
using Raven.Server.Logging;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Exceptions;
using Voron.Exceptions;

namespace Raven.Server
{
    public sealed class RavenServerStartup
    {
        private RequestRouter _router;
        private RavenServer _server;
        private long _requestId;
        private readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<RavenServerStartup>();

        public void Configure(IApplicationBuilder app, ILoggerFactory loggerFactory)
        {
            app.UseWebSockets(new WebSocketOptions
            {
                KeepAliveInterval = TimeSpan.FromHours(24)
            });

            _router = app.ApplicationServices.GetService<RequestRouter>();
            _server = app.ApplicationServices.GetService<RavenServer>();

            if (_server.Configuration.Http.UseResponseCompression)
            {
                // Enable automatic response compression for all routes that
                // are not studio's statics. The studio takes care of its own
                // compression.
                app.UseWhen(
                    context => context.Request.Path.StartsWithSegments("/studio") == false && context.Request.Path.StartsWithSegments("/wizard") == false,
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

        private async Task UnsafeRequestHandler(HttpContext context)
        {
            if (RoutesAllowedInUnsafeMode.Contains(context.Request.Path.Value))
            {
                await RequestHandler(context).ConfigureAwait(false);
                return;
            }

            context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;

            if (IsHtmlAcceptable(context))
            {
                context.Response.Headers[Constants.Headers.ContentType] = "text/html; charset=utf-8";

                await context.Response.WriteAsync(HtmlUtil.RenderUnsafePage())
                                      .ConfigureAwait(false);
                return;
            }

            context.Response.Headers[Constants.Headers.ContentType] = "application/json; charset=utf-8";
            using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, context.Response.Body))
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

                await writer.FlushAsync()
                            .ConfigureAwait(false);
            }
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
            $"Please find the RavenDB settings file *settings.json* in the server directory and fill in your certificate information in either { RavenConfiguration.GetKey(x => x.Security.CertificatePath) } or { RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec) }",
            $"If you would rather like to keep your server unsecured, please relax the { RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed) } setting to match the { RavenConfiguration.GetKey(x => x.Core.ServerUrls) } setting value."
        };

        private static readonly StringValues CacheControlHeaderValues = new[] { "must-revalidate", "no-cache" };

        private static readonly StringValues ContentTypeHeaderValue = "application/json; charset=utf-8";

        internal static readonly StringValues ServerVersionHeaderValue = RavenVersionAttribute.Instance.AssemblyVersion;

        private async Task RequestHandler(HttpContext context)
        {
            var requestHandlerContext = new RequestHandlerContext
            {
                HttpContext = context
            };
            Exception exception = null;
            Stopwatch sp = null;

            try
            {
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.Headers[Constants.Headers.ContentType] = ContentTypeHeaderValue;

                if (_server.ServerStore.Initialized == false)
                    await _server.ServerStore.InitializationCompleted.WaitAsync()
                                                                     .ConfigureAwait(false);

                sp = Stopwatch.StartNew();
                await _router.HandlePath(requestHandlerContext)
                             .ConfigureAwait(false);
                sp.Stop();
            }
            catch (Exception e)
            {
                try
                {
                    sp?.Stop();
                    exception = e;

                    CheckDatabaseShutdownAndThrowIfNeeded(requestHandlerContext, ref e);

                    CheckVersionAndWrapException(context, ref e);

                    MaybeSetExceptionStatusCode(context, _server.ServerStore, e);

                    if (context.RequestAborted.IsCancellationRequested)
                        return;

                    using (_server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        var djv = new DynamicJsonValue
                        {
                            [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = $"{context.Request.Path}{context.Request.QueryString}",
                            [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                            [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message,
                            [nameof(ExceptionDispatcher.ExceptionSchema.Error)] = e.ToString()
                        };

#if EXCEPTION_ERROR_HUNT
                    var f = Guid.NewGuid() + ".error";
                    File.WriteAllText(f,
                        $"{context.Request.Path}{context.Request.QueryString}" + Environment.NewLine + errorString);
#endif

                        MaybeAddAdditionalExceptionData(djv, e);

                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, context.Response.Body))
                        {
                            var json = ctx.ReadObject(djv, "exception");
                            writer.WriteObject(json);
                        }

#if EXCEPTION_ERROR_HUNT
                    File.Delete(f);
#endif
                    }
                }
                catch (Exception internalException)
                {
                    if (_logger.IsErrorEnabled)
                    {
                        _logger.Error($"Error during error handling of a failed request. Original error: {e}", internalException);
                    }

                    throw;
                }
            }
            finally
            {
                // check if TW has clients
                if (TrafficWatchManager.HasRegisteredClients)
                {
                    var database = requestHandlerContext.DatabaseName;
                    LogTrafficWatch(context, sp?.ElapsedMilliseconds ?? 0, database);
                }

                if (sp != null && requestHandlerContext.HttpContext.WebSockets.IsWebSocketRequest == false) // exclude web sockets
                {
                    var requestDuration = sp.ElapsedMilliseconds;
                    requestHandlerContext.RavenServer?.Metrics.Requests.UpdateDuration(requestDuration);
                    requestHandlerContext.DatabaseMetrics?.Requests.UpdateDuration(requestDuration);
                }

                if (_logger.IsDebugEnabled && SkipHttpLogging == false)
                {
                    _logger.Debug($"{context.Request.Method} {context.Request.Path.Value}{context.Request.QueryString.Value} - {context.Response.StatusCode} - {(sp?.ElapsedMilliseconds ?? 0):#,#;;0} ms", exception);
                }
            }
        }

        private static void CheckDatabaseShutdownAndThrowIfNeeded(RequestHandlerContext context, ref Exception e)
        {
            if (e is not OperationCanceledException) 
                return;

            var databaseShutdown = context.Database?.DatabaseShutdown ?? context.DatabaseContext?.DatabaseShutdown;
            var databaseShutdownCompleted = context.Database?.DatabaseShutdownCompleted;
            if (databaseShutdownCompleted is { IsSet: true} || databaseShutdown is { IsCancellationRequested: true })
                e = new DatabaseDisabledException("The database " + context.DatabaseName + " is shutting down", e);
        }

        private static void CheckVersionAndWrapException(HttpContext context, ref Exception e)
        {
            if (RequestRouter.TryGetClientVersion(context, out var version) == false)
                return;

            if (version.Major == '3')
            {
                e = new ClientVersionMismatchException(
                    $"RavenDB does not support interaction between Client API major version 3 and Server version {RavenVersionAttribute.Instance.MajorVersion} when major version does not match. Client: {version}. " +
                    $"Server: {RavenVersionAttribute.Instance.AssemblyVersion}",
                    e);
            }
            else if (HasInvalidCommandTypeException(e))
            {
                RequestRouter.CheckClientVersionAndWrapException(version, ref e);
            }

            static bool HasInvalidCommandTypeException(Exception e)
            {
                if (e is InvalidCommandTypeException)
                    return true;

                if (e is AggregateException ae)
                {
                    foreach (var innerException in ae.InnerExceptions)
                    {
                        if (HasInvalidCommandTypeException(innerException))
                            return true;
                    }
                }
                return e.InnerException != null && HasInvalidCommandTypeException(e.InnerException);
            }
        }

        /// <summary>
        /// LogTrafficWatch gets HttpContext, elapsed time and database name
        /// </summary>
        /// <param name="context"></param>
        /// <param name="elapsedMilliseconds"></param>
        /// <param name="database"></param>
        private void LogTrafficWatch(HttpContext context, long elapsedMilliseconds, string database)
        {
            var requestId = Interlocked.Increment(ref _requestId);
            var contextItem = context.Items["TrafficWatch"];
            (string CustomInfo, TrafficWatchChangeType Type) twTuple =
                ((string, TrafficWatchChangeType)?)contextItem ?? ("N/A", TrafficWatchChangeType.None);

            var timings = context.Items[nameof(QueryTimings)];

            var twn = new TrafficWatchHttpChange
            {
                TimeStamp = DateTime.UtcNow,
                RequestId = requestId, // counted only for traffic watch
                HttpMethod = context.Request.Method ?? "N/A", // N/A ?
                ElapsedMilliseconds = elapsedMilliseconds,
                ResponseStatusCode = context.Response.StatusCode,
                RequestUri = context.Request.GetEncodedUrl(),
                AbsoluteUri = $"{context.Request.Scheme}://{context.Request.Host}",
                DatabaseName = database ?? "N/A",
                CustomInfo = twTuple.CustomInfo,
                QueryTimings = timings != null ? (QueryTimings)timings : null,
                Type = twTuple.Type,
                ClientIP = context.Connection.RemoteIpAddress?.ToString(),
                CertificateThumbprint = context.Connection.ClientCertificate?.Thumbprint,
                RequestSizeInBytes = ((StreamWithTimeout)context.Items["RequestStream"])?.TotalRead ?? 0,
                ResponseSizeInBytes = ((StreamWithTimeout)context.Items["ResponseStream"])?.TotalWritten ?? 0,
            };

            TrafficWatchManager.DispatchMessage(twn);
        }

        private static void MaybeAddAdditionalExceptionData(DynamicJsonValue djv, Exception exception)
        {
            if (exception is IndexCompilationException indexCompilationException)
            {
                djv[nameof(IndexCompilationException.IndexDefinitionProperty)] = indexCompilationException.IndexDefinitionProperty;
                djv[nameof(IndexCompilationException.ProblematicText)] = indexCompilationException.ProblematicText;
                return;
            }

            if (exception is DocumentConflictException documentConflictException)
            {
                djv[nameof(DocumentConflictException.DocId)] = documentConflictException.DocId;
                djv[nameof(DocumentConflictException.LargestEtag)] = documentConflictException.LargestEtag;
            }

            if (exception is ConcurrencyException concurrencyException)
            {
                djv[nameof(ConcurrencyException.Id)] = concurrencyException.Id;
                djv[nameof(ConcurrencyException.ExpectedChangeVector)] = concurrencyException.ExpectedChangeVector;
                djv[nameof(ConcurrencyException.ActualChangeVector)] = concurrencyException.ActualChangeVector;
            }

            if (exception is RavenTimeoutException timeoutException)
            {
                djv[nameof(RavenTimeoutException.FailImmediately)] = timeoutException.FailImmediately;
            }

            if (exception is ClusterTransactionConcurrencyException { ConcurrencyViolations: { } } ctxConcurrencyException)
                djv[nameof(ClusterTransactionConcurrencyException.ConcurrencyViolations)] = new DynamicJsonArray(ctxConcurrencyException.ConcurrencyViolations.Select(c => c.ToJson()));
        }

        private static void MaybeSetExceptionStatusCode(HttpContext httpContext, ServerStore serverStore, Exception exception)
        {
            var response = httpContext.Response;

            if (response.HasStarted)
                return;

            if (exception is InsufficientTransportLayerProtectionException)
            {
                response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            if (exception.IsOutOfMemory() ||
                exception is HighDirtyMemoryException ||
                exception is VoronUnrecoverableErrorException ||
                exception is VoronErrorException ||
                exception is QuotaException ||
                exception is DiskFullException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            if (exception is DocumentConflictException ||
                exception is ConflictException ||
                exception is RachisConcurrencyException)
            {
                response.StatusCode = (int)HttpStatusCode.Conflict;
                return;
            }

            if (exception is DatabaseDisabledException ||
                exception is DatabaseRestoringException ||
                exception is DatabaseLoadFailureException ||
                exception is DatabaseLoadTimeoutException ||
                exception is DatabaseConcurrentLoadTimeoutException ||
                exception is NodeIsPassiveException ||
                exception is ClientVersionMismatchException ||
                exception is DatabaseSchemaErrorException ||
                exception is DatabaseIdleException
                )
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            if (exception is BadRequestException ||
                exception is RouteNotFoundException)
            {
                response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            if (exception is AuthorizationException)
            {
                response.StatusCode = (int)HttpStatusCode.Forbidden;
                return;
            }

            if (exception is PendingRollingIndexException)
            {
                response.StatusCode = 425; // TooEarly
                return;
            }

            if (exception is TimeoutException or RavenTimeoutException)
            {
                response.StatusCode = (int)HttpStatusCode.RequestTimeout;
                return;
            }

            if (exception is LicenseLimitException)
            {
                response.StatusCode = (int)HttpStatusCode.PaymentRequired;
                return;
            }

            if (exception is DatabaseNotRelevantException)
            {
                response.StatusCode = (int)HttpStatusCode.Gone;
                response.Headers["Cache-Control"] = CacheControlHeaderValues;
                return;
            }

            if (exception is IndexCompactionInProgressException)
            {
                response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                return;
            }

            response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
    }
}
