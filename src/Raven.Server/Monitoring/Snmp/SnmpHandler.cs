using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpHandler : RequestHandler
    {
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
