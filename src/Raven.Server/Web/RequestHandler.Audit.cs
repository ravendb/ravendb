using System;
using System.Diagnostics;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Http;
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
            if (LoggingSource.AuditLog.IsInfoEnabled)
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

                LogAuditFor(_context.DatabaseName ?? "Server", "TASK", line);
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

        public static string GetRequestIp(HttpContext httpContext) => IsLocalRequest(httpContext) ? Environment.MachineName : httpContext.Connection.RemoteIpAddress.ToString();

        public void LogAuditFor(string logger, string action, string target, Exception e = null)
        {
            LogAuditFor(logger, action, target, HttpContext, e);
        }
        
        public static void LogAuditFor(string logger, string action, string target, HttpContext httpContext, Exception e = null)
        {
            var auditLog = LoggingSource.AuditLog.GetLogger(logger, "Audit");
            Debug.Assert(auditLog.IsInfoEnabled, $"auditlog info is disabled");

            var clientCert = GetCurrentCertificate(httpContext);

            var sb = new StringBuilder();
            sb.Append(GetRequestIp(httpContext));
            sb.Append(", ");
            if (clientCert != null) 
                sb.Append($"CN={clientCert.GetDisplayName()} [{clientCert.Thumbprint}], ");
            else
                sb.Append("no certificate, ");

            sb.Append($"{action} {target}");

            if (e != null)
                sb.Append($", Exception: {e}");

            auditLog.Info(sb.ToString());
        }
    }
}
