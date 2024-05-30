using System;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

//https://devblogs.microsoft.com/dotnet/a-portable-way-to-get-gc-events-in-process-and-no-admin-privilege-with-10-lines-of-code-and-ability-to-dynamically-enable-disable-events/
public abstract class AbstractEventListener : System.Diagnostics.Tracing.EventListener
{
    protected abstract DotNetEventType? EventKeywords { get; }

    [Flags]
    public enum DotNetEventType
    {
        GC = 0x0000001,
        Threading = 0x10000,
        Exception = 0x8000,
        Contention = 0x4000
    }

    private EventSource _eventSourceDotNet;

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name.Equals("Microsoft-Windows-DotNETRuntime"))
        {
            if (EventKeywords == null)
                throw new InvalidOperationException($"{nameof(EventKeywords)} must be set!");

            EnableEvents(eventSource, EventLevel.Verbose, (EventKeywords)EventKeywords);
            _eventSourceDotNet = eventSource;
        }
    }

    public override void Dispose()
    {
        if (_eventSourceDotNet != null)
            DisableEvents(_eventSourceDotNet);

        base.Dispose();
    }
}
