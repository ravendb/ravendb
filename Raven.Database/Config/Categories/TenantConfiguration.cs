using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class TenantConfiguration : ConfigurationCategory
    {
        [Description("The time in seconds to allow a tenant database to be idle")]
        [DefaultValue(900)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Tenants/MaxIdleTimeForTenantDatabaseInSec")]
        [ConfigurationEntry("Raven/Tenants/MaxIdleTimeForTenantDatabase")]
        public TimeSetting MaxIdleTime { get; set; }

        [Description("The time in seconds to check for an idle tenant database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Tenants/FrequencyToCheckForIdleDatabasesInSec")]
        [ConfigurationEntry("Raven/Tenants/FrequencyToCheckForIdleDatabases")]
        public TimeSetting FrequencyToCheckForIdle { get; set; }
    }
}