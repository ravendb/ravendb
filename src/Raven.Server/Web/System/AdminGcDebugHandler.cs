using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.EventListener;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class AdminGcDebugHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/memory/allocations", "GET", AuthorizationStatus.Operator,
            // intentionally not calling it debug endpoint because it isn't valid for us
            // to do so in debug package (since we force a wait)
            IsDebugInformationEndpoint = false)]
        public async Task Allocations()
        {
            var delay = GetIntValueQueryString("delay", required: false) ?? 5;

            IReadOnlyCollection<GcAllocationsEventListener.AllocationInfo> allocations;
            using (var listener = new GcAllocationsEventListener())
            {
                await Task.Delay(TimeSpan.FromSeconds(delay));
                allocations = listener.Allocations;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                var first = true;
                foreach (var alloc in allocations.OrderByDescending(x => x.Allocations))
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;
                    writer.WritePropertyName(alloc.Type);
                    writer.WriteStartObject();
                    writer.WritePropertyName("Allocated");
                    writer.WriteString(new Size((long)alloc.Allocations, SizeUnit.Bytes).ToString());

                    var additionalLogging = alloc.Allocations != alloc.SmallObjectAllocations;
                    if (additionalLogging)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("AllocatedSmallObjects");
                        writer.WriteString(new Size((long)alloc.SmallObjectAllocations, SizeUnit.Bytes).ToString());
                        writer.WriteComma();
                        writer.WritePropertyName("AllocatedLargeObjects");
                        writer.WriteString(new Size((long)alloc.LargeObjectAllocations, SizeUnit.Bytes).ToString());
                    }
                    
                    writer.WriteComma();
                    writer.WritePropertyName("NumberOfAllocations");
                    writer.WriteInteger(alloc.NumberOfAllocations);

                    if (additionalLogging)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("NumberOfSmallObjectAllocations");
                        writer.WriteInteger(alloc.NumberOfSmallObjectAllocations);
                        writer.WriteComma();
                        writer.WritePropertyName("NumberOfLargeObjectAllocations");
                        writer.WriteInteger(alloc.NumberOfLargeObjectAllocations);
                    }

                    writer.WriteEndObject();
                }

                writer.WriteEndObject();
            }
        }

        [RavenAction("/admin/debug/memory/gc-events", "GET", AuthorizationStatus.Operator,
            // intentionally not calling it debug endpoint because it isn't valid for us
            // to do so in debug package (since we force a wait)
            IsDebugInformationEndpoint = false)]
        public async Task GcEvents()
        {
            var delay = GetIntValueQueryString("delay", required: false) ?? 10;

            IReadOnlyCollection<GcEventsHandler.GCEventBase> events;
            using (var listener = new GcEventsListener())
            {
                await Task.Delay(TimeSpan.FromSeconds(delay));
                events = listener.Events;
            }

            var sortedEvents = new SortedSet<GcEventsHandler.GCEventBase>(events, new EventComparerByDuration());

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("TopFiveByDuration");
                writer.WriteStartArray();

                var first = true;
                var count = 0;
                foreach (var @event in sortedEvents)
                {
                    if (++count > 5)
                        break;

                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    context.Write(writer, @event.ToJson());
                }

                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName("Events");
                writer.WriteStartArray();

                first = true;
                foreach (var @event in events.OrderBy(x => x.Start))
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    context.Write(writer, @event.ToJson());
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }

        internal class GcAllocationsEventListener : AbstractEventListener
        {
            internal const string AllocationEventName = "GCAllocationTick_V4";

            private readonly Dictionary<string, AllocationInfo> _allocations = new();

            public IReadOnlyCollection<AllocationInfo> Allocations => _allocations.Values;

            public GcAllocationsEventListener()
            {
                EnableEvents(DotNetEventType.GC);
            }

            public class AllocationInfo
            {
                private ulong? _allocations;

                public string Type;
                public ulong SmallObjectAllocations;
                public ulong LargeObjectAllocations;
                public long NumberOfSmallObjectAllocations;
                public long NumberOfLargeObjectAllocations;

                public ulong Allocations
                {
                    get
                    {
                        // used for ordering
                        _allocations ??= SmallObjectAllocations + LargeObjectAllocations;
                        return _allocations.Value;
                    }
                }

                public long NumberOfAllocations => NumberOfSmallObjectAllocations + NumberOfLargeObjectAllocations;
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                switch (eventData.EventName)
                {
                    case AllocationEventName:
                        var type = (string)eventData.Payload[5];
                        if (_allocations.TryGetValue(type, out var info) == false)
                        {
                            _allocations[type] = info = new AllocationInfo
                            {
                                Type = type
                            };
                        }
                        var allocations = (ulong)eventData.Payload[3];

                        var smallObjectAllocation = (uint)eventData.Payload[1] == 0x0;
                        if (smallObjectAllocation)
                        {
                            _allocations[type].SmallObjectAllocations += allocations;
                            _allocations[type].NumberOfSmallObjectAllocations++;
                        }
                        else
                        {
                            _allocations[type].LargeObjectAllocations += allocations;
                            _allocations[type].NumberOfLargeObjectAllocations++;
                        }

                        break;
                }
            }
        }
    }
}
