using System;
using System.Runtime;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminMemoryHandler : RequestHandler
    {
        [RavenAction("/admin/memory/gc", "GET", AuthorizationStatus.Operator)]
        public async Task CollectGarbage()
        {
            var loh = GetBoolValueQueryString("loh", required: false) ?? false;

            if (loh)
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

            var memoryBefore = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();
            var startTime = DateTime.UtcNow;

            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

            var actionTime = DateTime.UtcNow - startTime;
            var memoryAfter = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("BeforeGC");
                writer.WriteString(new Size(memoryBefore, SizeUnit.Bytes).ToString());
                writer.WriteComma();

                writer.WritePropertyName("AfterGC");
                writer.WriteString(new Size(memoryAfter, SizeUnit.Bytes).ToString());
                writer.WriteComma();

                writer.WritePropertyName("Freed");
                writer.WriteString(new Size(memoryBefore - memoryAfter, SizeUnit.Bytes).ToString());
                writer.WriteComma();

                writer.WritePropertyName("OperationTimeInSeconds");
                writer.WriteDouble(actionTime.TotalSeconds);

                writer.WriteEndObject();
            }
        }
    }
}
