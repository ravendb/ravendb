using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.EventListener;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Debug)]
public class DebugConfiguration : ConfigurationCategory
{
    [Description("Event listener logging mode.")]
    [DefaultValue(EventListenerMode.Off)]
    [ConfigurationEntry("Debug.EventListener.Mode", ConfigurationEntryScope.ServerWideOnly)]
    public EventListenerMode EventListenerMode { get; set; }

    [Description("A semicolon-separated list of event types by which the event listener logging entities will be filtered. If not specified, event listener entities with any type will be included. Example list: \"GC;GCSuspend;GCRestart;GCFinalizers;Contention\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("Debug.EventListener.EventTypes", ConfigurationEntryScope.ServerWideOnly)]
    public EventType[] EventTypes { get; set; }

    [Description("Minimum duration by which the event listenter logging entities will be filtered.")]
    [DefaultValue(0)]
    [TimeUnit(TimeUnit.Milliseconds)]
    [ConfigurationEntry("Debug.EventListener.MinimumDurationInMs", ConfigurationEntryScope.ServerWideOnly)]
    public TimeSetting MinimumDuration { get; set; }

    [Description("The duration on which we'll collect the allocations info.")]
    [DefaultValue(5000)]
    [TimeUnit(TimeUnit.Milliseconds)]
    [ConfigurationEntry("Debug.EventListener.AllocationsLoggingIntervalInMs", ConfigurationEntryScope.ServerWideOnly)]
    public TimeSetting AllocationsLoggingInterval { get; set; }

    [Description("Number of top allocation events to log")]
    [DefaultValue(5)]
    [ConfigurationEntry("Debug.EventListener.AllocationsLoggingCount", ConfigurationEntryScope.ServerWideOnly)]
    public int AllocationsLoggingCount { get; set; }
}
