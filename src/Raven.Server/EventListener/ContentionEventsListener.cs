using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public class ContentionEventsListener : AbstractEventListener
{
    private readonly ContentionEventsHandler _handler;
    private List<ContentionEventsHandler.ContentionEvent> _events = new();

    public IReadOnlyCollection<ContentionEventsHandler.ContentionEvent> Events => _events;

    public ContentionEventsListener()
    {
        _handler = new ContentionEventsHandler(e => _events.Add(e));
        EnableEvents(DotNetEventType.Contention);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        _handler.HandleEvent(eventData);
    }
}
