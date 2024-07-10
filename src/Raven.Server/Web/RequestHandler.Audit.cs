using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Logging;

namespace Raven.Server.Web
{
    public abstract partial class RequestHandler
    {
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

        public void LogAuditFor(string logger, string action, string target)
        {
            var auditLog = LoggingSource.AuditLog.GetLogger(logger, "Audit");
            Debug.Assert(auditLog.IsInfoEnabled, $"auditlog info is disabled");

            var clientCert = GetCurrentCertificate();
            
            var sb = new StringBuilder();
            sb.Append(RequestIp);
            sb.Append(", ");
            if (clientCert != null) 
                sb.Append($"CN={clientCert.Subject} [{clientCert.Thumbprint}], ");
            else
                sb.Append("no certificate, ");

            sb.Append($"{action} {target}");

            auditLog.Info(sb.ToString());
        }
    }
}
