using System.Collections.Generic;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

public class ContentionEventsListener : AbstractEventListener
{
    protected override DotNetEventType? EventKeywords => DotNetEventType.Contention;

    private readonly ContentionEventsHandler _handler;
    private List<ContentionEventsHandler.ContentionEvent> _events = new();

    public IReadOnlyCollection<ContentionEventsHandler.ContentionEvent> Events => _events;

    public ContentionEventsListener()
    {
        _handler = new ContentionEventsHandler(e => _events.Add(e));
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName == null)
            return;

        _handler.HandleEvent(eventData);
    }
}
