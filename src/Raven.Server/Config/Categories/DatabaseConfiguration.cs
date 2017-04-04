using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class DatabaseConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// This much time has to wait for the resource to become available when too much
        /// different resources get loaded at the same time
        /// </summary>
        [Description("The time in seconds to wait for a database to start loading when under load")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Databases/ConcurrentResourceLoadTimeoutInSec")]
        public TimeSetting ConcurrentResourceLoadTimeout { get; set; }

        /// <summary>
        /// specifies the maximum amount of databases that can be loaded simultaneously
        /// </summary>
        [DefaultValue(8)]
        [ConfigurationEntry("Raven/Databases/MaxConcurrentResourceLoads")]
        public int MaxConcurrentResourceLoads { get; set; }

        [DefaultValue(900)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Databases/MaxIdleTimeForTenantDatabaseInSec")]
        [LegacyConfigurationEntry("Raven/Databases/MaxIdleTimeForTenantDatabase")]
        public TimeSetting MaxIdleTime { get; set; }

        [Description("The time in seconds to check for an idle tenant database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Databases/FrequencyToCheckForIdleDatabasesInSec")]
        [LegacyConfigurationEntry("Raven/Databases/FrequencyToCheckForIdleDatabases")]
        public TimeSetting FrequencyToCheckForIdle { get; set; }
    }
}