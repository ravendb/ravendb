using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public interface IEventsHandler
{
    public bool HandleEvent(EventWrittenEventArgs eventData);

    public void Update(HashSet<EventType> eventTypes, long minimumDurationInMs);
}

public abstract class AbstractEventsHandler<TEvent> : IEventsHandler where TEvent : Event
{
    protected HashSet<EventType> EventTypes { get; set; }

    protected long MinimumDurationInMs { get; set; }

    protected abstract HashSet<EventType> DefaultEventTypes { get; }

    protected abstract Action<TEvent> OnEvent { get; }

    public abstract bool HandleEvent(EventWrittenEventArgs eventData);

    public void Update(HashSet<EventType> eventTypes, long minimumDurationInMs)
    {
        EventTypes = eventTypes ?? DefaultEventTypes;
        MinimumDurationInMs = minimumDurationInMs;
    }
}

public interface IDurationEvent
{
    public double DurationInMs { get; }
}
