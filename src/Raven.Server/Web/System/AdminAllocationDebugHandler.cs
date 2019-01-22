using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class AdminAllocationDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/memory/allocations", "GET", AuthorizationStatus.Operator,
            // intentionally not calling it debug endpoint because it isn't valid for us
            // to do so in debug package (since we force a wait)
            IsDebugInformationEndpoint = false)]
        public async Task Allocations()
        {
            var delay = GetIntValueQueryString("delay", required: false) ?? 5;

            IReadOnlyCollection<AllocationInfo> allocations;
            using (var listener = new Expensive_GcEventListener())
            {
                await Task.Delay(TimeSpan.FromSeconds(delay));
                allocations = listener.Allocations;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                var first = true;
                foreach (var alloc in allocations.OrderByDescending(x=>x.Allocations))
                {
                    if(first == false)
                        writer.WriteComma();

                    first = false;
                    writer.WritePropertyName(alloc.Type);
                    writer.WriteString(new Size((long)alloc.Allocations, SizeUnit.Bytes).ToString());
                }

                writer.WriteEndObject();
            }
        }

        public class AllocationInfo
        {
            public string Type;
            public ulong Allocations;
        }


        public sealed class Expensive_GcEventListener : EventListener
        {
            private const int GC_KEYWORD = 0x0000001;
           
            private Dictionary<string, AllocationInfo> _allocations = new Dictionary<string, AllocationInfo>();

            public IReadOnlyCollection<AllocationInfo> Allocations => _allocations.Values;
            protected override void OnEventSourceCreated(EventSource eventSource)
            {
                if (eventSource.Name.Equals("Microsoft-Windows-DotNETRuntime"))
                {
                    EnableEvents(eventSource, EventLevel.Verbose, (EventKeywords)GC_KEYWORD);
                }
            }
            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                switch (eventData.EventName)
                {
                    case "GCAllocationTick_V3":
                        var type = (string)eventData.Payload[5];
                        if (_allocations.TryGetValue(type, out var info) == false)
                        {
                            _allocations[type] = info = new AllocationInfo
                            {
                                Type = type
                            };
                        }
                        info.Allocations += (ulong)eventData.Payload[3];
                        break;
                }
            }
        }
    }
}
