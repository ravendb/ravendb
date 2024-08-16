using System;
using System.Diagnostics.Tracing;

namespace Raven.Server.EventListener;

//https://devblogs.microsoft.com/dotnet/a-portable-way-to-get-gc-events-in-process-and-no-admin-privilege-with-10-lines-of-code-and-ability-to-dynamically-enable-disable-events/
public abstract class AbstractEventListener : System.Diagnostics.Tracing.EventListener
{
    private EventSource _eventSourceDotNet;

    [Flags]
    public enum DotNetEventType
    {
        GC = 0x0000001,
        Threading = 0x10000,
        //Exception = 0x8000,
        Contention = 0x4000
    }

    protected void EnableEvents(DotNetEventType dotNetEventType)
    {
        if (_eventSourceDotNet != null)
            EnableEvents(_eventSourceDotNet, EventLevel.Verbose, (EventKeywords)dotNetEventType);
    }

    protected void DisableEvents()
    {
        DisableEvents(_eventSourceDotNet);
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name.Equals("Microsoft-Windows-DotNETRuntime"))
        {
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
