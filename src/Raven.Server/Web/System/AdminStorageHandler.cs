using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Web.System
{
    public sealed class AdminStorageHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/storage/environment/report", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = false)]
        public async Task SystemEnvironmentReport()
        {
            var details = GetBoolValueQueryString("details", required: false) ?? false;
            var env = ServerStore._env;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
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
        }

        [RavenAction("/admin/debug/storage/environment/scratch-buffer-info", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = false)]
        public async Task SystemScratchBufferPoolInfoReport()
        {
            var env = ServerStore._env;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Environment");
                writer.WriteString("Server");
                writer.WriteComma();

                writer.WritePropertyName("Type");
                writer.WriteString(nameof(StorageEnvironmentWithType.StorageEnvironmentType.System));
                writer.WriteComma();

                using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (var tx = env.ReadTransaction())
                using (ctx.OpenWriteTransaction())
                {
                    //Opening a write transaction to avoid concurrency problems (Issue #21088)
                    var sc = env.ScratchBufferPool.InfoForDebug(env.PossibleOldestReadTransaction(tx.LowLevelTransaction));
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(sc);
                    writer.WritePropertyName("Report");
                    writer.WriteObject(context.ReadObject(djv, "System"));
                }

                writer.WriteEndObject();
            }
        }
    }
}
