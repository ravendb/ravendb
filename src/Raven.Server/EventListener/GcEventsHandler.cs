using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using Sparrow.Json.Parsing;

namespace Raven.Server.EventListener;

public class GcEventsHandler : AbstractEventsHandler<GcEventsHandler.GCEventBase>
{
    private Dictionary<long, (DateTime DateTime, uint Generation, uint Reason)> _timeGcStartByIndex = new();
    private EventWrittenEventArgs _suspendData;
    private DateTime? _timeGcRestartStart;
    private DateTime? _timeGcFinalizersStart;

    protected override HashSet<EventType> DefaultEventTypes => EventListenerToLog.GcEvents;

    public GcEventsHandler(Action<GCEventBase> onEvent, HashSet<EventType> eventTypes = null, long minimumDurationInMs =0)
    {
        Update(eventTypes, minimumDurationInMs);
        OnEvent = onEvent;
    }

    protected override Action<GCEventBase> OnEvent { get; }

    public override bool HandleEvent(EventWrittenEventArgs eventData)
    {
        switch (eventData.EventName)
        {
            case "GCStart_V2":
                if (EventTypes.Contains(EventType.GC))
                {
                    var startIndex = long.Parse(eventData.Payload[0].ToString());
                    var generation = (uint)eventData.Payload[1];
                    var reason = (uint)eventData.Payload[2];
                    _timeGcStartByIndex[startIndex] = (eventData.TimeStamp, generation, reason);
                }
                
                return true;

            case "GCEnd_V1":
                if (EventTypes.Contains(EventType.GC))
                {
                    var endIndex = long.Parse(eventData.Payload[0].ToString());

                    if (_timeGcStartByIndex.TryGetValue(endIndex, out var tuple))
                    {
                        OnEvent.Invoke(new GCEvent(tuple.DateTime, eventData, endIndex, tuple.Generation, tuple.Reason));
                        _timeGcStartByIndex.Remove(endIndex);
                    }
                }
                
                return true;

            case "GCSuspendEEBegin_V1":
                if (EventTypes.Contains(EventType.GCSuspend))
                    _suspendData = eventData;

                return true;

            case "GCSuspendEEEnd_V1":
                if (_suspendData != null)
                {
                    var index = (uint)_suspendData.Payload[1];
                    var suspendReason = (uint)_suspendData.Payload[0];

                    OnEvent.Invoke(new GCSuspendEvent(_suspendData.TimeStamp, eventData, index, suspendReason));
                    _suspendData = null;
                }

                return true;

            case "GCRestartEEBegin_V1":
                _timeGcRestartStart = eventData.TimeStamp;
                break;

            case "GCRestartEEEnd_V1":
                if (_timeGcRestartStart != null)
                {
                    OnEvent.Invoke(new GCEventBase(EventType.GCRestart, _timeGcRestartStart.Value, eventData));
                    _timeGcRestartStart = null;
                }

                return true;

            case "GCFinalizersBegin_V1":
                _timeGcFinalizersStart = eventData.TimeStamp;
                return true;

            case "GCFinalizersEnd_V1":
                if (_timeGcFinalizersStart != null)
                {
                    OnEvent.Invoke(new GCEventBase(EventType.GCFinalizers, _timeGcFinalizersStart.Value, eventData));
                    _timeGcFinalizersStart = null;
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
            return $"{Type}, duration: {DurationInMs}";
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
            return $"{Type}, duration: {DurationInMs}, index: {Index}, generation: {Generation}, reason: {Reason}";
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
            return $"{Type}, duration: {DurationInMs}, index: {Index}, reason: {Reason}";
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

}
