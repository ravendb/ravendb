using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Web.System
{
    public class AdminStorageHandler : RequestHandler
    {
        [RavenAction("/admin/debug/storage/environment/report", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = false)]
        public Task SystemEnvironmentReport()
        {
            var details = GetBoolValueQueryString("details", required: false) ?? false;
            var env = ServerStore._env;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Environment");
                writer.WriteString("Server");
                writer.WriteComma();

                writer.WritePropertyName("Type");
                writer.WriteString(nameof(StorageEnvironmentWithType.StorageEnvironmentType.System));
                writer.WriteComma();

                using (var tx = env.ReadTransaction())
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(env.GenerateDetailedReport(tx, details));
                    writer.WritePropertyName("Report");
                    writer.WriteObject(context.ReadObject(djv, "System"));
                }

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}
