using System;
using System.Diagnostics;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Logging;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
        public void LogTaskToAudit(string description, long id, BlittableJsonReaderObject configuration)
        {
            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                DynamicJsonValue conf = GetCustomConfigurationAuditJson(description, configuration);
                var line = $"'{description}' with taskId: '{id}'";

                if (conf != null)
                {
                    var confString = string.Empty;
                    using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
                    {
                        confString = ctx.ReadObject(conf, "conf").ToString();
                    }

                    line += ($" Configuration: {confString}");
                }

                if (_context.DatabaseName != null)
                    LogAuditForDatabase(_context.DatabaseName, "TASK", line);
                else
                    LogAuditForServer("TASK", line);
            }
        }

        public static bool IsLocalRequest(HttpContext httpContext)
        {
            if (httpContext.Connection.RemoteIpAddress == null && httpContext.Connection.LocalIpAddress == null)
            {
                return true;
            }
            if (httpContext.Connection.RemoteIpAddress.Equals(httpContext.Connection.LocalIpAddress))
            {
                return true;
            }
            if (IPAddress.IsLoopback(httpContext.Connection.RemoteIpAddress))
            {
                return true;
            }
            return false;
        }

        public string RequestIp => GetRequestIp(HttpContext);

        public static string GetRequestIp(HttpContext httpContext)
        {
            var auth = httpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            if (auth?.IsSsoAuthenticated == true)
            {
                var (clientIp, _) = SsoForwardedForHelper.GetIps(httpContext, auth);
                return clientIp;
            }

            return IsLocalRequest(httpContext) ? Environment.MachineName : httpContext.Connection.RemoteIpAddress.ToString();
        }

        public void LogAuditForServer(string action, string target, Exception e = null)
        {
            LogAuditForServer(action, target, HttpContext, e);
        }

        public static void LogAuditForServer(string action, string target, HttpContext httpContext, Exception e = null)
        {
            var auditLogger = RavenLogManager.Instance.GetAuditLoggerForServer();
            LogAuditForInternal(auditLogger, action, target, httpContext, e);
        }

        public void LogAuditForDatabase(string databaseName, string action, string target, Exception e = null)
        {
            var auditLogger = RavenLogManager.Instance.GetAuditLoggerForDatabase(databaseName);
            LogAuditForInternal(auditLogger, action, target, HttpContext, e);
        }

        internal static void LogAuditForInternal(RavenAuditLogger auditLogger, string action, string target, HttpContext httpContext, Exception e = null)
        {
            Debug.Assert(auditLogger.IsAuditEnabled, "auditlog info is disabled");

            var clientCert = GetCurrentCertificate(httpContext);

            var auth = httpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

            var sb = new StringBuilder();
            sb.Append(GetRequestIp(httpContext));
            if (auth?.IsSsoAuthenticated == true)
            {
                var (_, proxyIp) = SsoForwardedForHelper.GetIps(httpContext, auth);
                string userDisplay = auth.Definition?.Name ?? auth.SsoUserIdentity;
                if (proxyIp != null)
                    sb.Append($" (via SSO proxy {proxyIp}, user: {userDisplay} ({auth.SsoUserIdentity}))");
                else
                    sb.Append($" (SSO user: {userDisplay} ({auth.SsoUserIdentity}))");
            }
            sb.Append(", ");
            if (clientCert != null)
                sb.Append($"CN={clientCert.GetDisplayName()} [{clientCert.Thumbprint}], ");
            else
                sb.Append("no certificate, ");

            sb.Append($"{action} {target}");

            if (e != null)
                sb.Append($", Exception: {e}");

            auditLogger.Audit(sb.ToString());
        }
    }
}
