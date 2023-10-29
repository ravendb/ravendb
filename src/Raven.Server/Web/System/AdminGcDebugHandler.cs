using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

                    context.Write(writer, @event.ToJson());
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
            private Dictionary<long, (DateTime DateTime, uint Generation, uint Reason)> timeGCStartByIndex = new();
            private EventWrittenEventArgs _suspendData;
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

            public class Event : IDynamicJson
            {
                private EventType Type { get; }

                private long OSThreadId { get; }

                private DateTime Start { get; }

                private DateTime End { get; }

                private double? _durationInMs;

                public Event(EventType type, DateTime start, EventWrittenEventArgs eventData)
                {
                    Type = type;
                    OSThreadId = eventData.OSThreadId;
                    Start = start;
                    End = eventData.TimeStamp;
                }

                public double DurationInMs
                {
                    get
                    {
                        _durationInMs ??= (End.Ticks - Start.Ticks) / 10.0 / 1000.0;
                        return _durationInMs.Value;
                    }
                }

                public virtual DynamicJsonValue ToJson()
                {
                    return new DynamicJsonValue
                    {
                        [nameof(Type)] = Type,
                        [nameof(OSThreadId)] = OSThreadId,
                        [nameof(Start)] = Start,
                        [nameof(End)] = End,
                        [nameof(DurationInMs)] = DurationInMs,
                    };
                }
            }

            private class GCEvent : Event
            {
                private long Index { get; }

                private uint Generation { get; }

                private string Reason { get; }

                public GCEvent(DateTime start, EventWrittenEventArgs eventData, long index, uint generation, uint reason)
                    : base(EventType.GC, start, eventData)
                {
                    Index = index;
                    Generation = generation;
                    Reason = GetGcReason(reason);
                }

                public override DynamicJsonValue ToJson()
                {
                    var json = base.ToJson();
                    json[nameof(Index)] = Index;
                    json[nameof(Generation)] = Generation;
                    json[nameof(Reason)] = Reason;
                    return json;
                }

                private static string GetGcReason(uint valueReason)
                {
                    switch (valueReason)
                    {
                        case 0x0:
                            return "Small object heap allocation";
                        case 0x1:
                            return "Induced";
                        case 0x2:
                            return "Low memory";
                        case 0x3:
                            return "Empty";
                        case 0x4:
                            return "Large object heap allocation";
                        case 0x5:
                            return "Out of space (for small object heap)";
                        case 0x6:
                            return "Out of space (for large object heap)";
                        case 0x7:
                            return "Induced but not forced as blocking";

                        default:
                            return null;
                    }
                }
            }

            private class GCSuspendEvent : Event
            {
                public uint Index { get; }

                private string Reason { get; }

                public GCSuspendEvent(DateTime start, EventWrittenEventArgs eventData, uint index, uint reason)
                    : base(EventType.GCSuspend, start, eventData)
                {
                    Index = index;
                    Reason = GetSuspendReason(reason);
                }

                public override DynamicJsonValue ToJson()
                {
                    var json = base.ToJson();
                    json[nameof(Index)] = Index;
                    json[nameof(Reason)] = Reason;
                    return json;
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

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                if (eventData.EventName == null)
                    return;

                switch (eventData.EventName)
                {
                    case "GCStart_V2":
                        var startIndex = long.Parse(eventData.Payload[0].ToString());
                        var generation = (uint)eventData.Payload[1];
                        var reason = (uint)eventData.Payload[2];
                        timeGCStartByIndex[startIndex] = (eventData.TimeStamp, generation, reason);
                        break;

                    case "GCEnd_V1":
                        var endIndex = long.Parse(eventData.Payload[0].ToString());

                        if (timeGCStartByIndex.TryGetValue(endIndex, out var tuple) == false)
                            return;

                        _events.Add(new GCEvent(tuple.DateTime, eventData, endIndex, tuple.Generation, tuple.Reason));
                        timeGCStartByIndex.Remove(endIndex);
                        break;

                    case "GCSuspendEEBegin_V1":
                        _suspendData = eventData;
                        break;

                    case "GCSuspendEEEnd_V1":
                        if (_suspendData == null)
                        {
                            Console.WriteLine($"WHAT???");
                            return;
                        }

                        var index = (uint)_suspendData.Payload[1];
                        var suspendReason = (uint)_suspendData.Payload[0];

                        _events.Add(new GCSuspendEvent(_suspendData.TimeStamp, eventData, index, suspendReason));
                        _suspendData = null;
                        break;

                    case "GCRestartEEBegin_V1":
                        timeGCRestartStart = eventData.TimeStamp;
                        break;

                    case "GCRestartEEEnd_V1":
                        if (timeGCRestartStart == null)
                            return;

                        _events.Add(new Event(EventType.GCRestart, timeGCRestartStart.Value, eventData));
                        timeGCRestartStart = null;
                        break;

                    case "GCFinalizersBegin_V1":
                        timeGCFinalizersStart = eventData.TimeStamp;
                        break;

                    case "GCFinalizersEnd_V1":
                        if (timeGCFinalizersStart == null)
                            return;

                        _events.Add(new Event(EventType.GCFinalizers, timeGCFinalizersStart.Value, eventData));
                        timeGCFinalizersStart = null;
                        break;
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
