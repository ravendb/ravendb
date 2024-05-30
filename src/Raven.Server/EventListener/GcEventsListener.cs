using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public class GcEventsListener : AbstractEventListener
{
    private readonly List<GcEventsHandler.GCEventBase> _events = new();
    private readonly GcEventsHandler _handler;

    protected override DotNetEventType? EventKeywords => DotNetEventType.GC;

    public IReadOnlyCollection<GcEventsHandler.GCEventBase> Events => _events;

    public GcEventsListener()
    {
        _handler = new GcEventsHandler(e => _events.Add(e));
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        _handler.HandleEvent(eventData);
    }
}
