using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
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

            IReadOnlyCollection<GcEventsEventListener.Event> events;
            using (var listener = new GcEventsEventListener())
            {
                await Task.Delay(TimeSpan.FromSeconds(delay));
                events = listener.Events;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("Events");
                writer.WriteStartArray();

                var first = true;
                foreach (var @event in events.OrderByDescending(x => x.DurationInMs))
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(GcEventsEventListener.Event.Type));
                    writer.WriteString(@event.Type.ToString());
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GcEventsEventListener.Event.DurationInMs));
                    writer.WriteDouble(@event.DurationInMs);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GcEventsEventListener.Event.Start));
                    writer.WriteString(@event.Start.ToString("yyyy/MM/dd HH:mm:ss.fff"));
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GcEventsEventListener.Event.End));
                    writer.WriteString(@event.End.ToString("yyyy/MM/dd HH:mm:ss.fff"));
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GcEventsEventListener.Event.OSThreadId));
                    writer.WriteInteger(@event.OSThreadId);

                    if (@event.Index != null)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(GcEventsEventListener.Event.Index));
                        writer.WriteInteger(@event.Index.Value);
                    }

                    if (@event.SuspendReason != null)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(GcEventsEventListener.Event.SuspendReason));
                        writer.WriteString(@event.SuspendReason);
                    }

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        private class GcAllocationsEventListener : Expensive_GcEventListener
        {
            private const string AllocationEventName = "GCAllocationTick_V4";

            private readonly Dictionary<string, AllocationInfo> _allocations = new();

            public IReadOnlyCollection<AllocationInfo> Allocations => _allocations.Values;

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

        private class GcEventsEventListener : Expensive_GcEventListener
        {
            private Dictionary<long, DateTime> timeGCStartByIndex = new();
            private DateTime? timeGCSuspendStart;
            private uint? _gcSuspendReason;
            private DateTime? timeGCRestartStart;
            private DateTime? timeGCFinalizersStart;
            private readonly List<Event> _events = new();

            public IReadOnlyCollection<Event> Events => _events;

            public enum EventType
            {
                GC,
                GCSuspend,
                GCRestart,
                GCFinalizers
            }

            public class Event
            {
                public EventType Type { get; set; }

                public long OSThreadId { get; set; }

                public DateTime Start { get; set; }

                public DateTime End { get; set; }

                public double DurationInMs { get; set; }

                public long? Index { get; set; }

                public string SuspendReason { get; set; }
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                if (eventData.EventName == null)
                    return;

                switch (eventData.EventName)
                {
                    case "GCStart_V2":
                        var startIndex = long.Parse(eventData.Payload[0].ToString());
                        timeGCStartByIndex[startIndex] = eventData.TimeStamp;
                        break;

                    case "GCEnd_V1":
                        var endIndex = long.Parse(eventData.Payload[0].ToString());

                        if (timeGCStartByIndex.TryGetValue(endIndex, out var start) == false)
                            return;

                        RegisterEvent(EventType.GC, start, eventData, index: endIndex);
                        break;

                    case "GCSuspendEEBegin_V1":
                        timeGCSuspendStart = eventData.TimeStamp;
                        _gcSuspendReason = (uint)eventData.Payload[0];
                        break;

                    case "GCSuspendEEEnd_V1":
                        if (timeGCSuspendStart == null)
                            return;

                        RegisterEvent(EventType.GCSuspend, timeGCSuspendStart.Value, eventData, suspendReason: GetSuspendReason(_gcSuspendReason));
                        timeGCSuspendStart = null;
                        _gcSuspendReason = null;
                        break;

                    case "GCRestartEEBegin_V1":
                        timeGCRestartStart = eventData.TimeStamp;
                        break;

                    case "GCRestartEEEnd_V1":
                        if (timeGCRestartStart == null)
                            return;

                        RegisterEvent(EventType.GCRestart, timeGCRestartStart.Value, eventData);
                        timeGCRestartStart = null;
                        break;

                    case "GCFinalizersBegin_V1":
                        timeGCFinalizersStart = eventData.TimeStamp;
                        break;

                    case "GCFinalizersEnd_V1":
                        if (timeGCFinalizersStart == null)
                            return;

                        RegisterEvent(EventType.GCFinalizers, timeGCFinalizersStart.Value, eventData);
                        timeGCFinalizersStart = null;
                        break;
                }
            }

            private void RegisterEvent(EventType type, DateTime start, EventWrittenEventArgs eventData, long? index = null, string suspendReason = null)
            {
                _events.Add(new Event
                {
                    Type = type,
                    Index = index,
                    OSThreadId = eventData.OSThreadId,
                    Start = start,
                    End = eventData.TimeStamp,
                    DurationInMs = (eventData.TimeStamp.Ticks - start.Ticks) / 10.0 / 1000.0,
                    SuspendReason = suspendReason
                });
            }

            private static string GetSuspendReason(uint? suspendReason)
            {
                switch (suspendReason)
                {
                    case 0x0:
                        return "Suspend for Other";
                    case 0x1:
                        return "Suspend for GC";
                    case 0x2:
                        return "Suspend for AppDomain shutdown";
                    case 0x3:
                        return "Suspend for code pitching";
                    case 0x4:
                        return "Suspend for shutdown";
                    case 0x5:
                        return "Suspend for debugger";
                    case 0x6:
                        return "Suspend for GC Prep";
                    case 0x7:
                        return "Suspend for debugger sweep";

                    default:
                        return null;
                }
            }
        }

        private abstract class Expensive_GcEventListener : EventListener
        {
            private const int GC_KEYWORD = 0x0000001;

            protected override void OnEventSourceCreated(EventSource eventSource)
            {
                if (eventSource.Name.Equals("Microsoft-Windows-DotNETRuntime"))
                {
                    EnableEvents(eventSource, EventLevel.Verbose, (EventKeywords)GC_KEYWORD);
                }
            }
        }
    }
}
