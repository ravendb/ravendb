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

        [DefaultValue(336)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Tombstones.RetentionTimeWithReplicationHubInHrs ", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Time (in hours) to save tombsones when we have hub replication definition.")]
        public TimeSetting RetentionTimeWithReplicationHub { get; set; }

        [DefaultValue(1440)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Tombstones.CleanupIntervalWithReplicationHubInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [Description("Time (in minutes) for new check for tombstone cleanup with hub definition.")]
        public TimeSetting CleanupIntervalWithReplicationHub { get; set; }
    }
}
