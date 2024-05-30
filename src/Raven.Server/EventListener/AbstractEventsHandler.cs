using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public abstract class AbstractEventsHandler<TEvent> where TEvent : Event
{
    protected abstract Action<TEvent> OnEvent { get; }

    public abstract bool HandleEvent(EventWrittenEventArgs eventData);
}

public interface IDurationEvent
{
    public double DurationInMs { get; }
}

public class EventComparerByDuration : IComparer<IDurationEvent>
{
    public int Compare(IDurationEvent x, IDurationEvent y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (ReferenceEquals(null, y))
            return 1;
        if (ReferenceEquals(null, x))
            return -1;

        return y.DurationInMs.CompareTo(x.DurationInMs);
    }
}
