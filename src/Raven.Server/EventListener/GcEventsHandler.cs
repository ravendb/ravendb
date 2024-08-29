using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public class GcEventsHandler : AbstractEventsHandler<GcEventsHandler.GCEventBase>
{
    private Dictionary<long, (DateTime DateTime, uint Generation, uint Reason)> _timeGcStartByIndex = new();
    private EventWrittenEventArgs _suspendData;
    private DateTime? _timeGcRestartStart;
    private DateTime? _timeGcFinalizersStart;

    protected override HashSet<EventType> DefaultEventTypes => EventListenerToLog.GcEvents;

    public GcEventsHandler(Action<GCEventBase> onEvent, HashSet<EventType> eventTypes = null, long minimumDurationInMs = 0)
    {
        Update(eventTypes, minimumDurationInMs);
        OnEvent = onEvent;
    }

    protected override Action<GCEventBase> OnEvent { get; }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case EventListener.Constants.EventNames.GC.GCStart:
                if (EventTypes.Contains(EventType.GC))
                {
                    var startIndex = long.Parse(eventData.Payload[0].ToString());
                    var generation = (uint)eventData.Payload[1];
                    var reason = (uint)eventData.Payload[2];
                    _timeGcStartByIndex[startIndex] = (eventData.TimeStamp, generation, reason);
                }
                
                return true;

            case EventListener.Constants.EventNames.GC.GCEnd:
                if (EventTypes.Contains(EventType.GC))
                {
                    var endIndex = long.Parse(eventData.Payload[0].ToString());

                    if (_timeGcStartByIndex.TryGetValue(endIndex, out var tuple))
                    {
                        var @event = new GCEvent(tuple.DateTime, eventData, endIndex, tuple.Generation, tuple.Reason);
                        if (@event.DurationInMs >= MinimumDurationInMs)
                            OnEvent.Invoke(@event);

                        _timeGcStartByIndex.Remove(endIndex);
                    }
                }
                
                return true;

            case EventListener.Constants.EventNames.GC.GCSuspendBegin:
                if (EventTypes.Contains(EventType.GCSuspend))
                    _suspendData = eventData;

                return true;

            case EventListener.Constants.EventNames.GC.GCSuspendEnd:
                if (EventTypes.Contains(EventType.GCSuspend) && _suspendData != null)
                {
                    var index = (uint)_suspendData.Payload[1];
                    var suspendReason = (uint)_suspendData.Payload[0];

                    var @event = new GCSuspendEvent(_suspendData.TimeStamp, eventData, index, suspendReason);
                    if (@event.DurationInMs >= MinimumDurationInMs)
                        OnEvent.Invoke(@event);

                    _suspendData = null;
                }

                return true;

            case EventListener.Constants.EventNames.GC.GCRestartBegin:
                if (EventTypes.Contains(EventType.GCRestart))
                    _timeGcRestartStart = eventData.TimeStamp;
                return true;

            case EventListener.Constants.EventNames.GC.GCRestartEnd:
                if (EventTypes.Contains(EventType.GCRestart) && _timeGcRestartStart != null)
                {
                    var @event = new GCEventBase(EventType.GCRestart, _timeGcRestartStart.Value, eventData);
                    if (@event.DurationInMs >= MinimumDurationInMs)
                        OnEvent.Invoke(@event);

                    _timeGcRestartStart = null;
                }

                return true;

            case EventListener.Constants.EventNames.GC.GCFinalizersBegin:
                if (EventTypes.Contains(EventType.GCFinalizers))
                    _timeGcFinalizersStart = eventData.TimeStamp;
                return true;

            case EventListener.Constants.EventNames.GC.GCFinalizersEnd:
                if (EventTypes.Contains(EventType.GCFinalizers) && _timeGcFinalizersStart != null)
                {
                    var @event = new GCEventBase(EventType.GCFinalizers, _timeGcFinalizersStart.Value, eventData);
                    if (@event.DurationInMs >= MinimumDurationInMs)
                        OnEvent.Invoke(@event);

                    _timeGcFinalizersStart = null;
                }

                return true;

            case EventListener.Constants.EventNames.GC.GCJoin:
                if (EventTypes.Contains(EventType.GCJoin))
                {
                    OnEvent.Invoke(new GCJoinEvent(EventType.GCJoin, eventData));
                }

                return true;
            
            case EventListener.Constants.EventNames.GC.GCHeapStats:
                if (EventTypes.Contains(EventType.GCHeapStats))
                {
                    OnEvent.Invoke(new GCHeapStatsEvent(EventType.GCHeapStats, eventData));
                }

                return true;
        }

        return false;
    }

    public class GCEventBase : Event, IDurationEvent
    {
        private long OSThreadId { get; }

        public DateTime Start { get; }

        private DateTime End { get; }

        private double? _durationInMs;

        public GCEventBase(EventType type, DateTime start, EventWrittenEventArgs eventData) : base(type)
        {
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

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(OSThreadId)] = OSThreadId;
            json[nameof(Start)] = Start;
            json[nameof(End)] = End;
            json[nameof(DurationInMs)] = DurationInMs;

            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, thread id: {OSThreadId}, duration: {DurationInMs}ms";
        }
    }

    private class GCEvent : GCEventBase
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

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, index: {Index}, generation: {Generation}, reason: {Reason}";
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

    private class GCSuspendEvent : GCEventBase
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

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, index: {Index}, reason: {Reason}";
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

    public class GCJoinEvent : GCEventBase
    {
        public string JoinTime { get; set; }

        public string JoinType { get; set; }

        public GCJoinEvent(EventType type, EventWrittenEventArgs eventData) : base(type, eventData.TimeStamp, eventData)
        {
            var joinTime = (uint)eventData.Payload[1];
            var joinType = (uint)eventData.Payload[2];

            JoinTime = GetJoinTime(joinTime);
            JoinType = GetJoinType(joinType);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(JoinTime)] = JoinTime;
            json[nameof(JoinType)] = JoinType;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}, join time: {JoinTime}, join type: {JoinType}";
        }

        private static string GetJoinTime(uint joinType)
        {
            switch (joinType)
            {
                case 0x0:
                    return "Join Start";
                case 0x1:
                    return "Join End";

                default:
                    return null;
            }
        }

        private static string GetJoinType(uint joinType)
        {
            switch (joinType)
            {
                case 0x0:
                    return "Last Join";
                case 0x1:
                    return "Join";
                case 0x2:
                    return "Restart";
                case 0x3:
                    return "First Reverse Join";
                case 0x4:
                    return "Reverse Join";

                default:
                    return null;
            }
        }
    }

    public class GCHeapStatsEvent : GCEventBase
    {
        public Size Generation0 { get; set; } // The size, in bytes, of generation 0 memory.
        public Size TotalPromotedGen0 { get; set; } // The number of bytes that are promoted from generation 0 to generation 1.
        public Size Generation1 { get; set; } // The size, in bytes, of generation 1 memory.
        public Size TotalPromotedGen1 { get; set; } // The number of bytes that are promoted from generation 1 to generation 2.
        public Size Generation2 { get; set; } // The size, in bytes, of generation 2 memory.
        public Size TotalPromotedGen2 { get; set; } // The number of bytes that survived in generation 2 after the last collection.
        public Size LargeObjectHeap { get; set; } // The size, in bytes, of the large object heap.
        public Size TotalPromotedLargeObjectHeap { get; set; } // The number of bytes that survived in the large object heap after the last collection.
        public Size Finalization { get; set; } // The total size, in bytes, of the objects that are ready for finalization.
        public ulong FinalizationPromotedCount { get; set; } // The number of objects that are ready for finalization.
        public uint PinnedObjectCount { get; set; } // The number of pinned (unmovable) objects.
        public uint SinkBlockCount { get; set; } // The number of synchronization blocks in use.
        public uint GCHandleCount { get; set; } // The number of garbage collection handles in use.
        public Size PinnedObjectHeap { get; set; } // The size, in bytes, of the pinned object heap.
        public Size TotalPromotedPinnedObjectHeap { get; set; } // The number of bytes that survived in the pinned object heap after the last collection.

        public GCHeapStatsEvent(EventType type, EventWrittenEventArgs eventData) : base(type, eventData.TimeStamp, eventData)
        {
            Generation0 = new Size((long)(ulong)eventData.Payload[0], SizeUnit.Bytes);
            TotalPromotedGen0 = new Size((long)(ulong)eventData.Payload[1], SizeUnit.Bytes);
            Generation1 = new Size((long)(ulong)eventData.Payload[2], SizeUnit.Bytes);
            TotalPromotedGen1 = new Size((long)(ulong)eventData.Payload[3], SizeUnit.Bytes);
            Generation2 = new Size((long)(ulong)eventData.Payload[4], SizeUnit.Bytes);
            TotalPromotedGen2 = new Size((long)(ulong)eventData.Payload[5], SizeUnit.Bytes);
            LargeObjectHeap = new Size((long)(ulong)eventData.Payload[6], SizeUnit.Bytes);
            TotalPromotedLargeObjectHeap = new Size((long)(ulong)eventData.Payload[7], SizeUnit.Bytes);
            Finalization = new Size((long)(ulong)eventData.Payload[8], SizeUnit.Bytes);
            FinalizationPromotedCount = (ulong)eventData.Payload[9];
            PinnedObjectCount = (uint)eventData.Payload[10];
            SinkBlockCount = (uint)eventData.Payload[11];
            GCHandleCount = (uint)eventData.Payload[12];
            PinnedObjectHeap = new Size((long)(ulong)eventData.Payload[14], SizeUnit.Bytes);
            TotalPromotedPinnedObjectHeap = new Size((long)(ulong)eventData.Payload[15], SizeUnit.Bytes);
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Generation0)] = Generation0.ToString();
            json[nameof(TotalPromotedGen0)] = TotalPromotedGen0.ToString();
            json[nameof(Generation1)] = Generation1.ToString();
            json[nameof(TotalPromotedGen1)] = TotalPromotedGen1.ToString();
            json[nameof(Generation2)] = Generation2.ToString();
            json[nameof(TotalPromotedGen2)] = TotalPromotedGen2.ToString();
            json[nameof(LargeObjectHeap)] = LargeObjectHeap.ToString();
            json[nameof(TotalPromotedLargeObjectHeap)] = TotalPromotedLargeObjectHeap.ToString();
            json[nameof(PinnedObjectHeap)] = PinnedObjectHeap.ToString();
            json[nameof(TotalPromotedPinnedObjectHeap)] = TotalPromotedPinnedObjectHeap.ToString();
            json[nameof(Finalization)] = Finalization.ToString();
            json[nameof(FinalizationPromotedCount)] = FinalizationPromotedCount;
            json[nameof(PinnedObjectCount)] = PinnedObjectCount;
            json[nameof(SinkBlockCount)] = SinkBlockCount;
            json[nameof(GCHandleCount)] = GCHandleCount;
            return json;
        }

        public override string ToString()
        {
            var str = base.ToString();
            return $"{str}," + Environment.NewLine +
                   $"gen 0: {Generation0}, promoted gen 0: {TotalPromotedGen0}," + Environment.NewLine +
                   $"gen 1: {Generation1}, promoted gen 1: {TotalPromotedGen1}," + Environment.NewLine +
                   $"gen 2: {Generation2}, promoted gen 2: {TotalPromotedGen2}," + Environment.NewLine +
                   $"loh: {LargeObjectHeap}, loh promoted: {TotalPromotedLargeObjectHeap}, poh: {PinnedObjectHeap}, poh promoted: {TotalPromotedPinnedObjectHeap}, " +
                   $"finalization: {Finalization}, finalization count: {FinalizationPromotedCount}, " +
                   $"pinned objects: {PinnedObjectCount}, synchronization blocks: {SinkBlockCount}, gc handles: {GCHandleCount}";

        }
    }
}
