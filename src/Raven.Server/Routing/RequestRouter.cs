// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Routing;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Extensions;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using static Raven.Server.RavenServer;
using HttpMethods = Raven.Client.Util.HttpMethods;

namespace Raven.Server.Routing
{
    public sealed class RequestRouter
    {
        public List<RouteInformation> AllRoutes;

        private static readonly string BrowserCertificateMessage = Environment.NewLine + "Your certificate store may be cached by the browser. " +
            "Create a new private browsing tab, which will not cache any certificates. (Ctrl+Shift+N in Chrome, Ctrl+Shift+P in Firefox)";

        public static readonly TimeSpan LastRequestTimeUpdateFrequency = TimeSpan.FromSeconds(15);
        private readonly RavenServer _ravenServer;
        private readonly MetricCounters _serverMetrics;
        private readonly Trie<RouteInformation> _trie;
        private DateTime _lastAuthorizedNonClusterAdminRequestTime;
        private DateTime _lastRequestTimeUpdated;
        private static readonly RavenLogger RequestLogger = RavenLogManager.Instance.GetLoggerForServer<RequestRouter>();

        public RequestRouter(Dictionary<string, RouteInformation> routes, RavenServer ravenServer)
        {
            _trie = Trie<RouteInformation>.Build(routes);
            _ravenServer = ravenServer;
            _serverMetrics = ravenServer.Metrics;
            AllRoutes = new List<RouteInformation>(routes.Values);
        }

        public static void AssertClientVersion(HttpContext context, Exception innerException)
        {
            // client in this context could be also a follower sending a command to his leader.
            if (TryGetClientVersion(context, out var clientVersion) == false)
                return;

            if (CheckClientVersionAndWrapException(clientVersion, ref innerException) == false)
                throw innerException;
        }

        public static bool CheckClientVersionAndWrapException(Version clientVersion, ref Exception innerException)
        {
            var currentServerVersion = RavenVersionAttribute.Instance;

            if (currentServerVersion.MajorVersion == clientVersion.Major &&
                currentServerVersion.BuildVersion >= clientVersion.Revision &&
                currentServerVersion.BuildVersion != ServerVersion.DevBuildNumber)
                return true;

            innerException = new ClientVersionMismatchException(
                $"Failed to make a request from a newer client with build version {clientVersion} to an older server with build version {RavenVersionAttribute.Instance.AssemblyVersion}.{Environment.NewLine}" +
                $"Upgrading this node might fix this issue.",
                innerException);
            return false;
        }

        public static bool TryGetClientVersion(HttpContext context, out Version version)
        {
            version = null;

            if (context.Request.Headers.TryGetValue(Constants.Headers.ClientVersion, out var versionHeader) == false)
                return false;

            return Version.TryParse(versionHeader, out version);
        }

        public RouteInformation GetRoute(string method, string path, out RouteMatch match)
        {
            var tryMatch = _trie.TryMatch(method, path);
            match = tryMatch.Match;
            return tryMatch.Value;
        }

        internal async ValueTask<(bool Authorized, AuthenticationStatus Status, string CertificateThumbprint)> TryAuthorizeAsync(RouteInformation route, HttpContext context, string databaseName)
        {
            var feature = context.Features.Get<IHttpAuthenticationFeature>() as AuthenticateConnection;

            if (feature.WrittenToAuditLog == 0) // intentionally racy, we'll check it again later
            {
                if (RavenLogManager.Instance.IsAuditEnabled)
                {
                    var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();

                    // only one thread will win it, technically, there can't really be threading
                    // here, because there is a single connection, but better to be safe
                    if (Interlocked.CompareExchange(ref feature.WrittenToAuditLog, 1, 0) == 0)
                    {
                        if (feature.WrongProtocolMessage != null)
                        {
                            auditLog.Audit($"Connection from {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} " +
                                           $"used the wrong protocol and will be rejected. {feature.WrongProtocolMessage}");
                        }
                        else
                        {
                            auditLog.Audit($"Connection from {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} " +
                                           $"with certificate '{feature.Certificate?.Subject} ({feature.Certificate?.Thumbprint})', status: {feature.StatusForAudit}, " +
                                           $"databases: [{string.Join(", ", feature.AuthorizedDatabases.Keys)}]");

                            var conLifetime = context.Features.Get<IConnectionLifetimeFeature>();
                            if (conLifetime != null)
                            {
                                var msg = $"Connection {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} closed. Was used with: " +
                                 $"with certificate '{feature.Certificate?.Subject} ({feature.Certificate?.Thumbprint})', status: {feature.StatusForAudit}, " +
                                 $"databases: [{string.Join(", ", feature.AuthorizedDatabases.Keys)}]";

                                CancellationTokenRegistration cancellationTokenRegistration = default;

                                cancellationTokenRegistration = conLifetime.ConnectionClosed.Register(() =>
                                {
                                    auditLog.Audit(msg);
                                    cancellationTokenRegistration.Dispose();
                                });
                            }
                        }
                    }
                }
            }

            if (CanAccessRoute(route, context, databaseName, feature) == false)
            {
                if (ShouldRetryToAuthenticateConnection(feature))
                {
                    var httpConnectionFeature = context.Features.Get<IHttpConnectionFeature>();

                    feature = _ravenServer.AuthenticateConnectionCertificate(feature.Certificate, httpConnectionFeature);
                    context.Features.Set<IHttpAuthenticationFeature>(feature);

                    if (CanAccessRoute(route, context, databaseName, feature))
                        return (true, feature.Status, feature.Certificate?.Thumbprint);
                }


                await UnlikelyFailAuthorizationAsync(context, databaseName, feature, route.AuthorizationStatus)
                            .ConfigureAwait(false);
                return (false, feature.Status, feature.Certificate?.Thumbprint);
            }

            if (feature.RequiresTwoFactor && _ravenServer.TwoFactor.ValidateTwoFactorRequestLimits(route, context, feature.TwoFactorAuthRegistration, out var twoFactorMsg) == false)
            {
                if (RavenLogManager.Instance.IsAuditEnabled)
                {
                    var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
                    auditLog.Audit($"Rejected request {context.Request.Method} {context.Request.GetFullUrl()} because: {twoFactorMsg}");
                }

                feature.WaitingForTwoFactorAuthentication();

                await UnlikelyFailAuthorizationAsync(context, databaseName, feature, route.AuthorizationStatus)
                            .ConfigureAwait(false);

                return (false, AuthenticationStatus.TwoFactorAuthFromInvalidLimit, feature.Certificate?.Thumbprint);
            }

            return (true, feature.Status, feature.Certificate?.Thumbprint);
        }


        internal bool CanAccessRoute(RouteInformation route, HttpContext context, string databaseName, RavenServer.AuthenticateConnection feature)
        {
            switch (route.AuthorizationStatus)
            {
                case AuthorizationStatus.UnauthenticatedClients:
                    var userWantsToAccessStudioMainPage = context.Request.Path == "/studio/index.html";
                    if (userWantsToAccessStudioMainPage)
                    {
                        switch (feature.Status)
                        {
                            case RavenServer.AuthenticationStatus.NoCertificateProvided:
                            case RavenServer.AuthenticationStatus.Expired:
                            case RavenServer.AuthenticationStatus.NotYetValid:
                            case RavenServer.AuthenticationStatus.None:
                            case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                            case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                                return false;
                        }
                    }

                    return true;

                case AuthorizationStatus.ClusterAdmin:
                case AuthorizationStatus.Operator:
                case AuthorizationStatus.ValidUser:
                case AuthorizationStatus.DatabaseAdmin:
                case AuthorizationStatus.RestrictedAccess:
                    switch (feature.Status)
                    {
                        case RavenServer.AuthenticationStatus.TwoFactorAuthFromInvalidLimit:
                        case RavenServer.AuthenticationStatus.TwoFactorAuthNotProvided:
                        case RavenServer.AuthenticationStatus.NoCertificateProvided:
                        case RavenServer.AuthenticationStatus.Expired:
                        case RavenServer.AuthenticationStatus.NotYetValid:
                        case RavenServer.AuthenticationStatus.None:
                            return false;

                        case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                        case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                            // we allow an access to the restricted endpoints with an unfamiliar certificate, since we will authorize it at the endpoint level
                            if (route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess)
                                return true;

                            goto case RavenServer.AuthenticationStatus.None;

                        case RavenServer.AuthenticationStatus.Allowed:
                            if (route.AuthorizationStatus == AuthorizationStatus.Operator || route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin)
                                goto case RavenServer.AuthenticationStatus.None;

                            if (databaseName == null)
                                return true;
                            if (feature.CanAccess(databaseName, route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin, route.EndpointType == EndpointType.Write))
                                return true;

                            goto case RavenServer.AuthenticationStatus.None;
                        case RavenServer.AuthenticationStatus.Operator:
                            if (route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin)
                                goto case RavenServer.AuthenticationStatus.None;
                            return true;

                        case RavenServer.AuthenticationStatus.ClusterAdmin:
                            return true;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                default:
                    ThrowUnknownAuthStatus(route);
                    return false; // never hit
            }
        }

        public async ValueTask HandlePath(RequestHandlerContext reqCtx)
        {
            var context = reqCtx.HttpContext;
            var tryMatch = _trie.TryMatch(context.Request.Method, context.Request.Path.Value);
            if (tryMatch.Value == null)
            {
                // CONNECT (https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/CONNECT)
                // starting from .NET 7 can be used by WS connections to establish a communication
                if (string.Equals(context.Request.Method, HttpMethods.Connect.Method, StringComparison.OrdinalIgnoreCase))
                    tryMatch = _trie.TryMatch(HttpMethods.Get.Method, context.Request.Path.Value);

                if (tryMatch.Value == null)
                {
                    var exception = new RouteNotFoundException($"There is no handler for path: {context.Request.Method} {context.Request.Path.Value}{context.Request.QueryString}");
                    AssertClientVersion(context, exception);
                    throw exception;
                }
            }

            reqCtx.RavenServer = _ravenServer;
            reqCtx.RouteMatch = tryMatch.Match;
            reqCtx.CheckForChanges = tryMatch.Value.CheckForChanges;

            var tuple = tryMatch.Value.TryGetHandler(reqCtx);
            var handler = tuple.Item1 ?? await tuple.Item2.ConfigureAwait(false);

            reqCtx.DatabaseMetrics?.Requests.RequestsPerSec.Mark();
            _serverMetrics.Requests.RequestsPerSec.Mark();

            Interlocked.Increment(ref _serverMetrics.Requests.ConcurrentRequestsCount);

            try
            {
                if (handler == null)
                {
                    if (RavenLogManager.Instance.IsAuditEnabled)
                    {
                        var auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();

                        auditLog.Audit($"Invalid request {context.Request.Method} {context.Request.Path} by " +
                            $"(Cert: {context.Connection.ClientCertificate?.Subject} ({context.Connection.ClientCertificate?.Thumbprint}) {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort})");
                    }

                    context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, context.Response.Body))
                    {
                        ctx.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Message"] = $"There is no handler for {context.Request.Method} {context.Request.Path}"
                            });
                    }

                    return;
                }

                var skipAuthorization = false;

                if (tryMatch.Value.CorsMode != CorsMode.None)
                {
                    RequestHandler.SetupCORSHeaders(context, reqCtx.RavenServer.ServerStore, tryMatch.Value.CorsMode);

                    // don't authorize preflight requests: https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html
                    skipAuthorization = context.Request.Method == "OPTIONS";
                }

                if (RequestHandler.CheckCSRF(context, reqCtx.RavenServer.ServerStore) == false)
                {
                    context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                    return;
                }

                var status = AuthenticationStatus.ClusterAdmin;
                string certificateThumbprint = null;

                try
                {
                    if (_ravenServer.Configuration.Security.AuthenticationEnabled && skipAuthorization == false)
                    {
                        var (authorized, authorizationStatus, thumbprint) = await TryAuthorizeAsync(tryMatch.Value, context, reqCtx.DatabaseName).ConfigureAwait(false);
                        status = authorizationStatus;
                        certificateThumbprint = thumbprint;

                        if (authorized == false)
                            return;
                    }
                }
                finally
                {
                    if (tryMatch.Value.SkipLastRequestTimeUpdate == false)
                    {
                        var now = SystemTime.UtcNow;

                        if (now - _lastRequestTimeUpdated >= LastRequestTimeUpdateFrequency)
                        {
                            _ravenServer.Statistics.LastRequestTime = now;
                            _lastRequestTimeUpdated = now;
                        }

                        _ravenServer.Statistics.UpdateLastCertificateRequestTime(certificateThumbprint, now);

                        if (now - _lastAuthorizedNonClusterAdminRequestTime >= LastRequestTimeUpdateFrequency &&
                            skipAuthorization == false)
                        {
                            switch (status)
                            {
                                case RavenServer.AuthenticationStatus.Allowed:
                                case RavenServer.AuthenticationStatus.Operator:
                                    {
                                        _ravenServer.Statistics.LastAuthorizedNonClusterAdminRequestTime = now;
                                        _lastAuthorizedNonClusterAdminRequestTime = now;
                                        break;
                                    }
                                case RavenServer.AuthenticationStatus.None:
                                case RavenServer.AuthenticationStatus.NoCertificateProvided:
                                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                                case RavenServer.AuthenticationStatus.UnfamiliarIssuer:
                                case RavenServer.AuthenticationStatus.ClusterAdmin:
                                case RavenServer.AuthenticationStatus.Expired:
                                case RavenServer.AuthenticationStatus.NotYetValid:
                                case RavenServer.AuthenticationStatus.TwoFactorAuthNotProvided:
                                case RavenServer.AuthenticationStatus.TwoFactorAuthFromInvalidLimit:
                                    break;

                                default:
                                    ThrowUnknownAuthStatus(status);
                                    break;
                            }
                        }
                    }
                }

                context.Response.Headers[Constants.Headers.ServerVersion] = RavenServerStartup.ServerVersionHeaderValue;
                context.Response.Headers[Constants.Headers.DatabaseClusterTransactionId] = reqCtx.ClusterTransactionId;

                if (reqCtx.Database != null)
                {
                    if (tryMatch.Value.DisableOnCpuCreditsExhaustion &&
                        _ravenServer.CpuCreditsBalance.FailoverAlertRaised.IsRaised())
                    {
                        await RejectRequestBecauseOfCpuThresholdAsync(context).ConfigureAwait(false);
                        return;
                    }

                    using (reqCtx.Database.DatabaseInUse(tryMatch.Value.SkipUsagesCount))
                    {
                        if (context.Request.Headers.TryGetValue(Constants.Headers.LastKnownClusterTransactionIndex, out var value)
                            && long.TryParse(value, out var index)
                            && index > reqCtx.Database.ClusterWideTransactionIndexWaiter.LastIndex)
                        {
                            Stopwatch sp = null;
                            if (RequestLogger.IsInfoEnabled)
                            {
                                sp = Stopwatch.StartNew();
                            }

                            await reqCtx.Database.ClusterWideTransactionIndexWaiter.WaitAsync(index, context.RequestAborted).ConfigureAwait(false);
                            
                            if (RequestLogger.IsInfoEnabled && sp != null)
                            {
                                RequestLogger.Info($"Took {sp} to wait for cluster transaction {index} (connId: {context.Connection.Id})");
                        }
                        }

                        await handler(reqCtx).ConfigureAwait(false);
                    }
                }
                else
                {
                    await handler(reqCtx).ConfigureAwait(false);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _serverMetrics.Requests.ConcurrentRequestsCount);
            }
        }

        private static async ValueTask DrainRequestAsync(JsonOperationContext ctx, HttpContext context)
        {
            if (context.Response.Headers.TryGetValue("Connection", out Microsoft.Extensions.Primitives.StringValues value) && value == "close")
                return; // don't need to drain it, the connection will close

            using (ctx.GetMemoryBuffer(out var buffer))
            {
                var requestBody = context.Request.Body;
                while (true)
                {
                    var read = await requestBody.ReadAsync(buffer.Memory.Memory).ConfigureAwait(false);
                    if (read == 0)
                        break;
                }
            }
        }

        private static async ValueTask RejectRequestBecauseOfCpuThresholdAsync(HttpContext context)
        {
            context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, context.Response.Body))
            {
                ctx.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Message"] = $"The request has been rejected because the CPU credits balance on this instance has been exhausted. See /debug/cpu-credits endpoint for details."
                    });
            }
        }

        public static async ValueTask UnlikelyFailAuthorizationAsync(HttpContext context, string database,
            RavenServer.AuthenticateConnection feature,
            AuthorizationStatus authorizationStatus)
        {
            string message;
            int statusCode = (int)HttpStatusCode.Forbidden;
            if (feature == null ||
                feature.Status == RavenServer.AuthenticationStatus.None ||
                feature.Status == RavenServer.AuthenticationStatus.NoCertificateProvided)
            {
                message = "This server requires client certificate for authentication, but none was provided by the client. Did you forget to install the certificate?";
                message += context.Request.IsFromClientApi() == false ? BrowserCertificateMessage : string.Empty;
            }
            else
            {
                var name = feature.Certificate.FriendlyName;
                if (string.IsNullOrWhiteSpace(name))
                    name = feature.Certificate.Subject;
                if (string.IsNullOrWhiteSpace(name))
                    name = feature.Certificate.ToString(false);

                name += $"(Thumbprint: {feature.Certificate.Thumbprint})";

                if (feature.Status == RavenServer.AuthenticationStatus.UnfamiliarCertificate)
                {
                    message = $"The supplied client certificate '{name}' is unknown to the server. In order to register your certificate please contact your system administrator.";
                    message += context.Request.IsFromClientApi() == false ? BrowserCertificateMessage : string.Empty;
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.UnfamiliarIssuer)
                {
                    message = $"The supplied client certificate '{name}' is unknown to the server but has a known Public Key Pinning Hash. Will not use it to authenticate because the issuer is unknown. ";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Allowed)
                {
                    message = $"Could not authorize access to {(database ?? "the server")} using provided client certificate '{name}'.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Operator)
                {
                    message = $"Insufficient security clearance to access {(database ?? "the server")} using provided client certificate '{name}'.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Expired)
                {
                    message = $"The supplied client certificate '{name}' has expired on {feature.Certificate.NotAfter:D}. Please contact your system administrator in order to obtain a new one.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.NotYetValid)
                {
                    message = $"The supplied client certificate '{name}' cannot be used before {feature.Certificate.NotBefore:D}";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.TwoFactorAuthNotProvided)
                {
                    statusCode = (int)HttpStatusCode.PreconditionRequired;
                    message = $"The supplied client certificate '{name}' requires two factor authorization to be valid. Please POST the relevant TOTP value to /authentication/2fa";
                }
                else
                {
                    message = "Access to the server was denied.";
                }
            }

            switch (authorizationStatus)
            {
                case AuthorizationStatus.ClusterAdmin:
                    message += " ClusterAdmin access is required but not given to this certificate";
                    break;

                case AuthorizationStatus.Operator:
                    message += " Operator/ClusterAdmin access is required but not given to this certificate";
                    break;

                case AuthorizationStatus.DatabaseAdmin:
                    message += " DatabaseAdmin access is required but not given to this certificate";
                    break;
            }

            context.Response.StatusCode = statusCode;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            await using (var writer = new AsyncBlittableJsonTextWriter(ctx, context.Response.Body))
            {
                await DrainRequestAsync(ctx, context).ConfigureAwait(false);

                if (RavenServerStartup.IsHtmlAcceptable(context))
                {
                    context.Response.StatusCode = (int)HttpStatusCode.Redirect;
                    context.Response.Headers["Location"] = "/auth-error.html?err=" + Uri.EscapeDataString(message);
                    return;
                }

                ctx.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = "InvalidAuth",
                        ["Message"] = message
                    });
            }
        }

        private bool ShouldRetryToAuthenticateConnection(AuthenticateConnection authenticateConnection) =>
            authenticateConnection.Status == AuthenticationStatus.UnfamiliarCertificate &&
            _ravenServer.ServerStore.LastCertificateUpdateTime.HasValue &&
            _ravenServer.ServerStore.LastCertificateUpdateTime.Value > authenticateConnection.CreatedAt;

        [DoesNotReturn]
        private static void ThrowUnknownAuthStatus(RouteInformation route)
        {
            throw new ArgumentOutOfRangeException("Unknown route auth status: " + route.AuthorizationStatus);
        }

        [DoesNotReturn]
        private static void ThrowUnknownAuthStatus(RavenServer.AuthenticationStatus status)
        {
            throw new ArgumentOutOfRangeException("Unknown auth status: " + status);
        }
    }
}
