// -----------------------------------------------------------------------
//  <copyright file="RequestRouter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Routing;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Routing
{
    public class RequestRouter
    {
        private readonly Trie<RouteInformation> _trie;
        private readonly RavenServer _ravenServer;
        private readonly MetricCounters _serverMetrics;
        public List<RouteInformation> AllRoutes;

        public RequestRouter(Dictionary<string, RouteInformation> routes, RavenServer ravenServer)
        {
            _trie = Trie<RouteInformation>.Build(routes);
            _ravenServer = ravenServer;
            _serverMetrics = ravenServer.Metrics;
            AllRoutes = new List<RouteInformation>(routes.Values);
        }


        public RouteInformation GetRoute(string method, string path, out RouteMatch match)
        {
            var tryMatch = _trie.TryMatch(method, path);
            match = tryMatch.Match;
            return tryMatch.Value;
        }

        public async ValueTask HandlePath(RequestHandlerContext reqCtx)
        {
            var context = reqCtx.HttpContext;
            var tryMatch = _trie.TryMatch(context.Request.Method, context.Request.Path.Value);
            if (tryMatch.Value == null)
            {
                var exception = new RouteNotFoundException($"There is no handler for path: {context.Request.Method} {context.Request.Path.Value}{context.Request.QueryString}");
                AssertClientVersion(context, exception);
                throw exception;
            }

            reqCtx.RavenServer = _ravenServer;
            reqCtx.RouteMatch = tryMatch.Match;

            var tuple = tryMatch.Value.TryGetHandler(reqCtx);
            var handler = tuple.Item1 ?? await tuple.Item2;

            reqCtx.Database?.Metrics?.Requests.RequestsPerSec.Mark();

            _serverMetrics.Requests.RequestsPerSec.Mark();

            Interlocked.Increment(ref _serverMetrics.Requests.ConcurrentRequestsCount);

            try
            {
                _ravenServer.Statistics.LastRequestTime = SystemTime.UtcNow;

                if (handler == null)
                {
                    var auditLog = LoggingSource.AuditLog.IsInfoEnabled ? LoggingSource.AuditLog.GetLogger("RequestRouter", "Audit") : null;

                    if (auditLog != null)
                    {
                        auditLog.Info($"Invalid request {context.Request.Method} {context.Request.Path} by " +
                            $"(Cert: {context.Connection.ClientCertificate?.Subject} ({context.Connection.ClientCertificate?.Thumbprint}) {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort})");
                    }

                    context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
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

                if (_ravenServer.Configuration.Security.AuthenticationEnabled)
                {
                    var authResult = TryAuthorize(tryMatch.Value, context, reqCtx.Database);
                    if (authResult == false)
                        return;
                }

                if (reqCtx.Database != null)
                {
                    using (reqCtx.Database.DatabaseInUse(tryMatch.Value.SkipUsagesCount))
                    {
                        if (context.Response.Headers.TryGetValue(Constants.Headers.LastKnownClusterTransactionIndex, out var value)
                            && long.TryParse(value, out var index)
                            && index < reqCtx.Database.RachisLogIndexNotifications.LastModifiedIndex)
                        {
                            await reqCtx.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, context.RequestAborted);
                        }

                        await handler(reqCtx);
                    }
                }
                else
                {
                    await handler(reqCtx);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _serverMetrics.Requests.ConcurrentRequestsCount);
            }
        }

        public static bool TryGetClientVersion(HttpContext context, out Version version)
        {
            version = null;

            if (context.Request.Headers.TryGetValue(Constants.Headers.ClientVersion, out var versionHeader) == false)
                return false;

            return Version.TryParse(versionHeader, out version);
        }

        public static void AssertClientVersion(HttpContext context, Exception innerException)
        {
            // client in this context could be also a follower sending a command to his leader.
            if (TryGetClientVersion(context, out var clientVersion))
            {
                var currentServerVersion = RavenVersionAttribute.Instance;

                if (currentServerVersion.MajorVersion != clientVersion.Major || currentServerVersion.BuildVersion < clientVersion.Revision || currentServerVersion.BuildVersion == ServerVersion.DevBuildNumber || (clientVersion.Revision >= 40 && clientVersion.Revision < 50))
                {
                    throw new ClientVersionMismatchException(
                        $"Failed to make a request from a newer client with build version {clientVersion} to an older server with build version {RavenVersionAttribute.Instance.AssemblyVersion}.{Environment.NewLine}" +
                        $"Upgrading this node might fix this issue.",
                        innerException);
                }
            }
        }

        private bool TryAuthorize(RouteInformation route, HttpContext context, DocumentDatabase database)
        {
            var feature = context.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            if (feature.WrittenToAuditLog == 0) // intentionally racy, we'll check it again later
            {
                var auditLog = LoggingSource.AuditLog.IsInfoEnabled ? LoggingSource.AuditLog.GetLogger("RequestRouter", "Audit") : null;

                if (auditLog != null)
                {
                    // only one thread will win it, technically, there can't really be threading
                    // here, because there is a single connection, but better to be safe
                    if (Interlocked.CompareExchange(ref feature.WrittenToAuditLog, 1, 0) == 0)
                    {
                        if (feature.WrongProtocolMessage != null)
                        {
                            auditLog.Info($"Connection from {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} " +
                                $"used the wrong protocol and will be rejected. {feature.WrongProtocolMessage}");
                        }
                        else
                        {
                            auditLog.Info($"Connection from {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} " +
                                $"with certificate '{feature.Certificate?.Subject} ({feature.Certificate?.Thumbprint})', status: {feature.StatusForAudit}, " +
                                $"databases: [{string.Join(", ", feature.AuthorizedDatabases.Keys)}]");

                            var conLifetime = context.Features.Get<IConnectionLifetimeFeature>();
                            if(conLifetime != null)
                            {
                                var msg = $"Connection {context.Connection.RemoteIpAddress}:{context.Connection.RemotePort} closed. Was used with: " +
                                 $"with certificate '{feature.Certificate?.Subject} ({feature.Certificate?.Thumbprint})', status: {feature.StatusForAudit}, " +
                                 $"databases: [{string.Join(", ", feature.AuthorizedDatabases.Keys)}]";

                                conLifetime.ConnectionClosed.Register(() =>
                                {
                                    auditLog.Info(msg);
                                });
                            }

                        }
                    }
                }
            }

            var authenticationStatus = feature?.Status;
            switch (route.AuthorizationStatus)
            {
                case AuthorizationStatus.UnauthenticatedClients:
                    var userWantsToAccessStudioMainPage = context.Request.Path == "/studio/index.html";
                    if (userWantsToAccessStudioMainPage)
                    {
                        switch (authenticationStatus)
                        {
                            case null:
                            case RavenServer.AuthenticationStatus.NoCertificateProvided:
                            case RavenServer.AuthenticationStatus.Expired:
                            case RavenServer.AuthenticationStatus.NotYetValid:
                            case RavenServer.AuthenticationStatus.None:
                            case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                                UnlikelyFailAuthorization(context, database?.Name, feature, route.AuthorizationStatus);
                                return false;
                        }
                    }

                    return true;
                case AuthorizationStatus.ClusterAdmin:
                case AuthorizationStatus.Operator:
                case AuthorizationStatus.ValidUser:
                case AuthorizationStatus.DatabaseAdmin:
                case AuthorizationStatus.RestrictedAccess:
                    switch (authenticationStatus)
                    {
                        case null:
                        case RavenServer.AuthenticationStatus.NoCertificateProvided:
                        case RavenServer.AuthenticationStatus.Expired:
                        case RavenServer.AuthenticationStatus.NotYetValid:
                        case RavenServer.AuthenticationStatus.None:
                            UnlikelyFailAuthorization(context, database?.Name, feature, route.AuthorizationStatus);
                            return false;

                        case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                            // we allow an access to the restricted endpoints with an unfamilier certificate, since we will authorize it at the endpoint level
                            if (route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess)
                                return true; 
                            goto case null;
                         
                        case RavenServer.AuthenticationStatus.Allowed:
                            if (route.AuthorizationStatus == AuthorizationStatus.Operator || route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin)
                                goto case RavenServer.AuthenticationStatus.None;

                            if (database == null)
                                return true;
                            if (feature.CanAccess(database.Name, route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin))
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

        private static void ThrowUnknownAuthStatus(RouteInformation route)
        {
            throw new ArgumentOutOfRangeException("Unknown route auth status: " + route.AuthorizationStatus);
        }

        public static void UnlikelyFailAuthorization(HttpContext context, string database,
            RavenServer.AuthenticateConnection feature,
            AuthorizationStatus authorizationStatus)
        {
            string message;
            if (feature == null ||
                feature.Status == RavenServer.AuthenticationStatus.None ||
                feature.Status == RavenServer.AuthenticationStatus.NoCertificateProvided)
            {
                message = "This server requires client certificate for authentication, but none was provided by the client.";
            }
            else
            {
                var name = feature.Certificate.FriendlyName;
                if (string.IsNullOrWhiteSpace(name))
                    name = feature.Certificate.Subject;
                if (string.IsNullOrWhiteSpace(name))
                    name = feature.Certificate.ToString(false);

                name += "(Thumbprint: " + feature.Certificate.Thumbprint + ")";

                if (feature.Status == RavenServer.AuthenticationStatus.UnfamiliarCertificate)
                {
                    message = "The supplied client certificate '" + name + "' is unknown to the server. In order to register your certificate please contact your system administrator.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Allowed)
                {
                    message = "Could not authorize access to " + (database ?? "the server") + " using provided client certificate '" + name + "'.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Operator)
                {
                    message = "Insufficient security clearance to access " + (database ?? "the server") + " using provided client certificate '" + name + "'.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.Expired)
                {
                    message = "The supplied client certificate '" + name + "' has expired on " + feature.Certificate.NotAfter.ToString("D") + ". Please contact your system administrator in order to obtain a new one.";
                }
                else if (feature.Status == RavenServer.AuthenticationStatus.NotYetValid)
                {
                    message = "The supplied client certificate '" + name + "'cannot be used before " + feature.Certificate.NotBefore.ToString("D");
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

            context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(ctx, context.Response.Body))
            {
                DrainRequest(ctx, context);

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

        private static void DrainRequest(JsonOperationContext ctx, HttpContext context)
        {
            if (context.Response.Headers.TryGetValue("Connection", out StringValues value) && value == "close")
                return; // don't need to drain it, the connection will close 

            using (ctx.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
            {
                var requestBody = context.Request.Body;
                while (true)
                {
                    var read = requestBody.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Buffer.Count);
                    if (read == 0)
                        break;
                }
            }
        }
    }
}
