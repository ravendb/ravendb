using System.ComponentModel;

using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Tombstones)]
    public class TombstoneConfiguration : ConfigurationCategory
    {
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Tombstones.CleanupIntervalInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Time (in minutes) between tombstone cleanups.")]
        public TimeSetting CleanupInterval { get; set; }

        [DefaultValue(14)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Tombstones.RetentionTimeWithReplicationHubInDays ", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Time (in days) to save tombsones when we have hub replication definition.")]
        public TimeSetting RetentionTimeWithReplicationHub { get; set; }

        [DefaultValue(24)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Tombstones.CleanupIntervalWithReplicationHubInHrs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Time (in hours) for new check for tombstone cleanup with hub definition.")]
        public TimeSetting CleanupIntervalWithReplicationHub { get; set; }
    }
}
