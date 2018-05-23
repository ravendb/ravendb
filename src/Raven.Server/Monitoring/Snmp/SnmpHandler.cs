using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpHandler : RequestHandler
    {
        [RavenAction("/monitoring/snmp", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task Get()
        {
            if (ServerStore.Configuration.Monitoring.Snmp.Enabled == false)
                throw new InvalidOperationException($"SNMP Monitoring is not enabled. Please set the '{RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)}' configuration option to true.");

            if (ServerStore.LicenseManager.CanUseSnmpMonitoring() == false)
                throw new InvalidOperationException("Your license does not allow SNMP monitoring to be used.");

            var oid = GetQueryStringValueAndAssertIfSingleAndNotEmpty("oid");

            var data = Server.SnmpWatcher.GetData(oid);
            if (data == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Value");
                writer.WriteString(data.ToString());

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/monitoring/snmp/oids", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetOids()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(SnmpOids.Server)] = SnmpOids.Server.ToJson(),
                        [nameof(SnmpOids.Cluster)] = SnmpOids.Cluster.ToJson(),
                        [nameof(SnmpOids.Databases)] = SnmpOids.Databases.ToJson(ServerStore, context)
                    };

                    var json = context.ReadObject(djv, "snmp/oids");

                    writer.WriteObject(json);
                }
            }

            return Task.CompletedTask;
        }
    }
}
