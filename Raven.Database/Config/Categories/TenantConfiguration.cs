using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class TenantConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// This much time has to wait for the resource to become available when too much
        /// different resources get loaded at the same time
        /// </summary>
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Tenants/ConcurrentResourceLoadTimeoutInSec")]
        public TimeSetting ConcurrentResourceLoadTimeout { get; set; }

        /// <summary>
        /// specifies the maximum amount of tenants that can be loaded simultaenously
        /// </summary>
        [DefaultValue(8)]
        [ConfigurationEntry("Raven/Tenants/MaxConcurrentResourceLoads")]
        public int MaxConcurrentResourceLoads { get; set; }

        [DefaultValue(900)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Tenants/MaxIdleTimeForTenantDatabaseInSec")]
        [ConfigurationEntry("Raven/Tenants/MaxIdleTimeForTenantDatabase")]
        public TimeSetting MaxIdleTime { get; set; }

        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Tenants/FrequencyToCheckForIdleDatabasesInSec")]
        [ConfigurationEntry("Raven/Tenants/FrequencyToCheckForIdleDatabases")]
        public TimeSetting FrequencyToCheckForIdle { get; set; }
    }
}