using System.ComponentModel;
using Raven.Client.ServerWide.Operations.EventListener;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.EventListener;

namespace Raven.Server.Config.Categories;

[ConfigurationCategory(ConfigurationCategoryType.Etw)]
public class EventListenerConfiguration : ConfigurationCategory
{
    [Description("Event listener logging mode.")]
    [DefaultValue(EventListenerMode.Off)]
    [ConfigurationEntry("EventListener.Mode", ConfigurationEntryScope.ServerWideOnly)]
    public EventListenerMode EventListenerMode { get; set; }

    [Description("A semicolon-separated list of event types by which the event listener logging entities will be filtered. If not specified, event listener entities with any type will be included. Example list: \"GC;GCSuspend;GCRestart;GCFinalizers;Contention\".")]
    [DefaultValue(null)]
    [ConfigurationEntry("EventListener.EventTypes", ConfigurationEntryScope.ServerWideOnly)]
    public EventType[] EventTypes { get; set; }

    [Description("Minimum duration by which the event listenter logging entities will be filtered.")]
    [DefaultValue(0)]
    [TimeUnit(TimeUnit.Milliseconds)]
    [ConfigurationEntry("EventListener.MinimumDurationInMs", ConfigurationEntryScope.ServerWideOnly)]
    public TimeSetting MinimumDuration { get; set; }
}
