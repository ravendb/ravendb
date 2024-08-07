using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;

namespace Raven.Server.EventListener;

public class EventsListener : AbstractEventListener
{
    private readonly List<IEventsHandler> _handlers = new();
    private readonly Dictionary<string, IEventsHandler> _handlerByEventName = new();
    private DotNetEventType _dotNetEventType;

    public EventsListener(HashSet<EventType> eventTypes, long minimumDurationInMs, Action<Event> onEvent)
    {
        _handlers.Add(new GcEventsHandler(onEvent, eventTypes, minimumDurationInMs));
        _handlers.Add(new ContentionEventsHandler(onEvent, eventTypes, minimumDurationInMs));

        _dotNetEventType = GetDotNetEventTypes(eventTypes);
        EnableEvents(_dotNetEventType);
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

    public void Update(HashSet<EventType> eventTypes, long minimumDurationInMs)
    {
        foreach (var handler in _handlers)
        {
            handler.Update(eventTypes, minimumDurationInMs);
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
                    dotNetEventType = (dotNetEventType ?? DotNetEventType.GC) | DotNetEventType.GC;
                    break;
                case EventType.Contention:
                    dotNetEventType = (dotNetEventType ?? DotNetEventType.Contention) | DotNetEventType.Contention;
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
