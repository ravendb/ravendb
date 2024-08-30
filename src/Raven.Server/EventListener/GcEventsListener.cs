using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public class GcEventsListener : AbstractEventListener
{
    private readonly List<GcEventsHandler.GCEventBase> _events = new();
    private readonly GcEventsHandler _handler;

    public IReadOnlyCollection<GcEventsHandler.GCEventBase> Events => _events;

    private readonly HashSet<EventType> _eventsToLog =
    [
        EventType.GC,
        EventType.GCSuspend,
        EventType.GCRestart,
        EventType.GCFinalizers,
        EventType.GCHeapStats
    ];

    public GcEventsListener()
    {
        _handler = new GcEventsHandler(e => _events.Add(e), _eventsToLog);
        EnableEvents(DotNetEventType.GC);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        _handler.HandleEvent(eventData);
    }
}
