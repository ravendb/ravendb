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
        [Description("The time in seconds to allow a tenant database to be idle")]
        [DefaultValue(15)]
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
        [ConfigurationEntry("Raven/Databases/MaxIdleTimeForTenantDatabase")]
        public TimeSetting MaxIdleTime { get; set; }

        [Description("The time in seconds to check for an idle tenant database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Databases/FrequencyToCheckForIdleDatabasesInSec")]
        [ConfigurationEntry("Raven/Databases/FrequencyToCheckForIdleDatabases")]
        public TimeSetting FrequencyToCheckForIdle { get; set; }

        [Description("The maximum document size after which it will get into the huge documents collection")]
        [DefaultValue(5 * 1024 * 1024)]
        [ConfigurationEntry("Raven/Databases/MaxWarnSizeHugeDocuments")]
        public int MaxWarnSizeHugeDocuments { get; set; }

        [Description("The maximum size of the huge documents collection")]
        [DefaultValue(100)]
        [ConfigurationEntry("Raven/Databases/MaxCollectionSizeHugeDocuments")]
        public int MaxCollectionSizeHugeDocuments { get; set; }
    }
}