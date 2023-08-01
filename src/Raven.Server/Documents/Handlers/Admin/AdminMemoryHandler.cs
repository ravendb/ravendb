using System;
using System.Runtime;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class AdminMemoryHandler : ServerRequestHandler
    {
        [RavenAction("/admin/memory/gc", "GET", AuthorizationStatus.Operator)]
        public async Task CollectGarbage()
        {
            var loh = GetBoolValueQueryString("loh", required: false) ?? false;

            if (loh)
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);

            var gcMemoryInfo = GC.GetGCMemoryInfo(GCKind.Any);

            long memoryBefore = 0;
            long memoryAfter = 0;
            long durationInMs = 0;

            for (int index = 0; index < gcMemoryInfo.GenerationInfo.Length; index++)
            {
                var info = gcMemoryInfo.GenerationInfo[index];
                memoryBefore += info.SizeBeforeBytes;
                memoryAfter += info.SizeAfterBytes;
            }

            for (int index = 0; index < gcMemoryInfo.PauseDurations.Length; index++)
            {
                var duration = gcMemoryInfo.PauseDurations[index];
                durationInMs += duration.Milliseconds;
            }

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

                writer.WritePropertyName("DurationInMs");
                writer.WriteDouble(durationInMs);
                writer.WriteComma();

                writer.WritePropertyName("PinnedObjectsCount");
                writer.WriteInteger(gcMemoryInfo.PinnedObjectsCount);
                writer.WriteComma();

                writer.WritePropertyName("FinalizationPendingCount");
                writer.WriteInteger(gcMemoryInfo.FinalizationPendingCount);
                writer.WriteComma();

                writer.WritePropertyName("PauseTimePercentage");
                writer.WriteString($"{gcMemoryInfo.PauseTimePercentage}%");

                writer.WriteEndObject();
            }
        }
    }
}
