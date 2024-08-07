using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.EventListener;

public class EventListenerToLog : IDynamicJson
{
    public static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("EventListener");
    private readonly SemaphoreSlim _sm = new(1, 1);

    private EventsListener _listener;
    private EventListenerConfiguration _configuration;

    public static readonly HashSet<EventType> GcEvents = [EventType.GC, EventType.GCSuspend, EventType.GCRestart, EventType.GCFinalizers];
    public static readonly HashSet<EventType> ContentionTypes = [EventType.Contention];
    private static readonly HashSet<EventType> AllEvents = new(GcEvents.Concat(ContentionTypes));

    private EventListenerToLog()
    {
    }

    public static EventListenerToLog Instance = new();

    public bool LogToFile => _configuration.EventListenerMode == EventListenerMode.ToLogFile && Logger.IsOperationsEnabled;

    public void UpdateConfiguration(EventListenerConfiguration configuration)
    {
        _sm.Wait();

        try
        {
            _configuration = configuration;

            var effectiveEventTypes = (_configuration.EventTypes == null || _configuration.EventTypes.Length == 0)
                ? AllEvents
                : new HashSet<EventType>(_configuration.EventTypes);

            if (LogToFile == false)
            {
                _listener?.Dispose();
                _listener = null;
            }
            else
            {
                _listener ??= new EventsListener(effectiveEventTypes, _configuration.MinimumDurationInMs,
                    onEvent: e =>
                    {
                        if (LogToFile)
                            Logger.Operations(e.ToString());
                    });
                _listener.Update(effectiveEventTypes, _configuration.MinimumDurationInMs);
            }
        }
        finally
        {
            _sm.Release();
        }
    }

    public class EventListenerConfiguration
    {
        public EventListenerMode EventListenerMode { get; set; }

        public EventType[] EventTypes { get; set; }

        public long MinimumDurationInMs { get; set; }

        public bool Persist { get; set; }
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(EventListenerConfiguration.EventListenerMode)] = Instance._configuration.EventListenerMode,
            [nameof(EventListenerConfiguration.EventTypes)] = Instance._configuration.EventTypes == null ? null : new DynamicJsonArray(Instance._configuration.EventTypes),
            [nameof(EventListenerConfiguration.MinimumDurationInMs)] = Instance._configuration.MinimumDurationInMs
        };
    }
}
