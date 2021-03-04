using System;
using System.Net;
using System.Threading.Tasks;
using Lextm.SharpSnmpLib;
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
        [RavenAction("/monitoring/snmp", "GET", AuthorizationStatus.Operator)]
        public async Task Get()
        {
            AssertSnmp();

            var oid = GetQueryStringValueAndAssertIfSingleAndNotEmpty("oid");

            var data = Server.SnmpWatcher.GetData(oid);
            if (data == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Value");
                writer.WriteString(data.ToString());

                writer.WriteEndObject();
            }
        }

        [RavenAction("/monitoring/snmp/bulk", "GET", AuthorizationStatus.Operator)]
        public async Task GetBulk()
        {
            AssertSnmp();

            var oids = GetStringValuesQueryString("oid");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await BulkInternal(oids.ToArray(), context);
            }
        }

        [RavenAction("/monitoring/snmp/bulk", "POST", AuthorizationStatus.Operator)]
        public async Task PostBulk()
        {
            AssertSnmp();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "oids");
                if (json.TryGet("OIDs", out BlittableJsonReaderArray array) == false)
                    ThrowRequiredPropertyNameInRequest("OIDs");

                var length = array?.Length ?? 0;
                var oids = new string[length];
                for (var i = 0; i < length; i++)
                    oids[i] = array[i].ToString();

                await BulkInternal(oids, context);
            }
        }

        [RavenAction("/monitoring/snmp/oids", "GET", AuthorizationStatus.Operator)]
        public async Task GetOids()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
        }

        private async ValueTask BulkInternal(string[] oids, JsonOperationContext context)
        {
            var results = new (string Oid, ISnmpData Data)[oids.Length];
            for (var i = 0; i < oids.Length; i++)
            {
                var oid = oids[i];

                try
                {
                    var data = Server.SnmpWatcher.GetData(oid);

                    results[i] = (oid, data);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not get data for OID '{oid}'. Reason: {e.Message}", e);
                }
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                writer.WriteStartArray();

                var first = true;
                foreach (var result in results)
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName("OID");
                    writer.WriteString(result.Oid);
                    writer.WriteComma();

                    writer.WritePropertyName("Value");
                    if (result.Data != null)
                        writer.WriteString(result.Data.ToString());
                    else
                        writer.WriteNull();

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        private void AssertSnmp()
        {
            if (ServerStore.Configuration.Monitoring.Snmp.Enabled == false)
                throw new InvalidOperationException($"SNMP Monitoring is not enabled. Please set the '{RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)}' configuration option to true.");

            if (ServerStore.LicenseManager.CanUseSnmpMonitoring(withNotification: false) == false)
                throw new InvalidOperationException("Your license does not allow SNMP monitoring to be used.");
        }
    }
}
