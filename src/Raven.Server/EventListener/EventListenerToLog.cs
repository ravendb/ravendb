using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.ServerWide.Operations.EventListener;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Sparrow.Logging;

namespace Raven.Server.EventListener;

public class EventListenerToLog
{
    public static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("EventListener");
    private readonly SemaphoreSlim _sm = new(1, 1);

    private EventsListener _listener;
    private EventListenerMode _eventListenerMode;

    public static readonly HashSet<EventType> GcEvents = [EventType.GC, EventType.GCSuspend, EventType.GCRestart, EventType.GCFinalizers];
    public static readonly HashSet<EventType> ContentionTypes = [EventType.Contention];
    private static readonly HashSet<EventType> AllEvents = new(GcEvents.Concat(ContentionTypes));

    private EventListenerToLog()
    {
    }

    public static EventListenerToLog Instance = new();

    public bool LogToFile => _eventListenerMode == EventListenerMode.ToLogFile && Logger.IsOperationsEnabled;

    public void UpdateConfiguration(EventListenerConfiguration configuration)
    {
        _sm.Wait();

        try
        {
            _eventListenerMode = configuration.EventListenerMode;
            var eventTypes = configuration.EventTypes;
            var minimumDurationInMs = configuration.MinimumDuration.GetValue(TimeUnit.Milliseconds);

            var effectiveEventTypes = (eventTypes == null || eventTypes.Length == 0)
                ? AllEvents
                : new HashSet<EventType>(eventTypes);

            if (LogToFile == false)
            {
                _listener?.Dispose();
                _listener = null;
            }
            else
            {
                _listener ??= new EventsListener(Logger, effectiveEventTypes, minimumDurationInMs);
                _listener.Update(effectiveEventTypes, minimumDurationInMs);
            }
        }
        finally
        {
            _sm.Release();
        }
    }
}
