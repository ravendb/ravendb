using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class TenantConfiguration : ConfigurationCategory
    {
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