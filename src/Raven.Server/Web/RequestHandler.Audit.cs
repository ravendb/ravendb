﻿using System;
using System.Diagnostics;
using System.Net;
using System.Text;
using Raven.Server.Logging;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

                LogAuditFor(_context.DatabaseName ?? "Server", "TASK", line);
            }
        }

        public bool IsLocalRequest()
        {
            if (HttpContext.Connection.RemoteIpAddress == null && HttpContext.Connection.LocalIpAddress == null)
            {
                return true;
            }
            if (HttpContext.Connection.RemoteIpAddress.Equals(HttpContext.Connection.LocalIpAddress))
            {
                return true;
            }
            if (IPAddress.IsLoopback(HttpContext.Connection.RemoteIpAddress))
            {
                return true;
            }
            return false;
        }

        public string RequestIp => IsLocalRequest() ? Environment.MachineName : HttpContext.Connection.RemoteIpAddress.ToString();

        public void LogAuditFor(string logger, string action, string target, Exception e = null)
        {
            var auditLogger = RavenLogManager.Instance.GetAuditLoggerForServer();

            Debug.Assert(auditLogger.IsAuditEnabled, $"auditlog info is disabled");

            var clientCert = GetCurrentCertificate();

            var sb = new StringBuilder();
            sb.Append(RequestIp);
            sb.Append(", ");
            if (clientCert != null) 
                sb.Append($"CN={clientCert.Subject} [{clientCert.Thumbprint}], ");
            else
                sb.Append("no certificate, ");

            sb.Append($"{action} {target}");

            if (e != null)
                sb.Append($", Exception: {e}");

            auditLogger.Audit(sb.ToString());
        }
    }
}
