using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;

namespace Raven.Server.EventListener;

public class EventsListener : AbstractEventListener
{
    private readonly List<IEventsHandler> _handlers = new();
    private readonly Dictionary<string, IEventsHandler> _handlerByEventName = new();
    private DotNetEventType _dotNetEventType;

    private readonly Dictionary<string, AllocationsHandler.AllocationInfo> _allocations = new();
    private readonly StringBuilder _sb = new();

    private Stopwatch _stopwatchSinceLastAllocation;
    private long _allocationsLoggingIntervalInMs;
    private int _allocationsLoggingCount;
    private readonly InternalEvent _internalEvent = new();

    public EventsListener(HashSet<EventType> eventTypes, long minimumDurationInMs, long allocationsLoggingIntervalInMs, int allocationsLoggingCount, Action<Event> onEvent)
    {
        _allocationsLoggingIntervalInMs = allocationsLoggingIntervalInMs;
        _allocationsLoggingCount = allocationsLoggingCount;
        _handlers.Add(new GcEventsHandler(onEvent, eventTypes, minimumDurationInMs));
        _handlers.Add(new AllocationsHandler(OnAllocationEvent(onEvent), eventTypes, minimumDurationInMs));
        _handlers.Add(new ContentionEventsHandler(onEvent, eventTypes, minimumDurationInMs));
        _handlers.Add(new ThreadsHandler(onEvent, eventTypes, minimumDurationInMs));

        _dotNetEventType = GetDotNetEventTypes(eventTypes);
        EnableEvents(_dotNetEventType);
    }

    private Action<AllocationsHandler.AllocationInfo> OnAllocationEvent(Action<Event> onEvent)
    {
        return e =>
        {
            _stopwatchSinceLastAllocation ??= Stopwatch.StartNew();

            if (_allocations.TryGetValue(e.AllocationType, out var allocation) == false)
            {
                _allocations[e.AllocationType] = e;
            }
            else
            {
                allocation.SmallObjectAllocations += e.SmallObjectAllocations;
                allocation.NumberOfSmallObjectAllocations += e.NumberOfSmallObjectAllocations;
                allocation.LargeObjectAllocations += e.LargeObjectAllocations;
                allocation.NumberOfLargeObjectAllocations += e.NumberOfLargeObjectAllocations;
            }

            if (_stopwatchSinceLastAllocation.ElapsedMilliseconds >= _allocationsLoggingIntervalInMs)
            {
                var count = _allocationsLoggingCount;
                _sb.Clear();

                _sb.Append($"Top {_allocationsLoggingCount} allocations for the past {_allocationsLoggingIntervalInMs:#,#;;0}ms: ");
                _sb.AppendLine();

                var first = true;
                foreach (var alloc in _allocations.Values.OrderByDescending(x => x.Allocations))
                {
                    if (first == false)
                        _sb.AppendLine();

                    first = false;
                    
                    _sb.Append(alloc);
                    if (--count <= 0)
                        break;
                }

                _internalEvent.SetString(_sb.ToString());
                onEvent.Invoke(_internalEvent);
                _stopwatchSinceLastAllocation.Restart();
                _allocations.Clear();
            }
        };
    }

    private class InternalEvent : Event
    {
        private string _str;

        public InternalEvent() : base(EventType.Allocations)
        {
        }

        public void SetString(string str)
        {
            _str = str;
        }

        public override string ToString()
        {
            return _str;
        }
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        if (_handlerByEventName.TryGetValue(eventData.EventName, out var handlerToUse))
        {
            handlerToUse?.HandleEvent(eventData);
            return;
        }

        foreach (var handler in _handlers)
        {
            if (handler.HandleEvent(eventData))
            {
                _handlerByEventName[eventData.EventName] = handler;
                return;
            }
        }

        _handlerByEventName[eventData.EventName] = null;
    }

    public void Update(HashSet<EventType> eventTypes, long minimumDurationInMs, long allocationsLoggingIntervalInMs, int allocationsLoggingCount)
    {
        _allocationsLoggingIntervalInMs = allocationsLoggingIntervalInMs;
        _allocationsLoggingCount = allocationsLoggingCount;

        foreach (var handler in _handlers)
        {
            handler.Update(eventTypes, minimumDurationInMs);
        }

        if (eventTypes.Contains(EventType.Allocations) == false)
        {
            _allocations.Clear();
            _stopwatchSinceLastAllocation = null;
        }

        var newDotNetEventType = GetDotNetEventTypes(eventTypes);
        if (_dotNetEventType == newDotNetEventType)
            return;

        DisableEvents();
        _dotNetEventType = newDotNetEventType;
        EnableEvents(_dotNetEventType);
    }

    private static DotNetEventType GetDotNetEventTypes(HashSet<EventType> eventTypes)
    {
        DotNetEventType? dotNetEventType = null;

        foreach (var eventType in eventTypes)
        {
            switch (eventType)
            {
                case EventType.GC:
                case EventType.GCSuspend:
                case EventType.GCRestart:
                case EventType.GCFinalizers:
                case EventType.GCJoin:
                case EventType.Allocations:
                    dotNetEventType = (dotNetEventType ?? DotNetEventType.GC) | DotNetEventType.GC;
                    break;
                case EventType.Contention:
                    dotNetEventType = (dotNetEventType ?? DotNetEventType.Contention) | DotNetEventType.Contention;
                    break;
                case EventType.ThreadPoolWorkerThreadStart:
                case EventType.ThreadPoolWorkerThreadWait:
                case EventType.ThreadPoolWorkerThreadStop:
                case EventType.ThreadPoolMinMaxThreads:
                case EventType.ThreadPoolWorkerThreadAdjustment:
                case EventType.ThreadPoolWorkerThreadAdjustmentSample:
                case EventType.ThreadPoolWorkerThreadAdjustmentStats:
                case EventType.ThreadCreating:
                case EventType.ThreadCreated:
                case EventType.ThreadRunning:
                case EventType.GCCreateConcurrentThread_V1:
                    dotNetEventType = (dotNetEventType ?? DotNetEventType.Threading) | DotNetEventType.Threading;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        if (dotNetEventType == null)
            throw new InvalidOperationException($"Failed to determine which event type to log, {string.Join(", ", eventTypes.Select(et => et.ToString()))}");

        return dotNetEventType.Value;
    }
}
