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
    }
}
