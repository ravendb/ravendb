using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class DatabaseConfiguration : ConfigurationCategory
    {
        [Description("The time in seconds to wait before canceling query operation")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.QueryOperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting QueryOperationTimeout { get; set; }

        [Description("The time in seconds to wait before canceling terms indexing operation")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.IndexTermsOperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting IndexTermsOperationTimeout { get; set; }

        [Description("The time in seconds to wait before canceling indexing terms operation")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.DeleteDocsOperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting DeleteDocsOperationTimeout { get; set; }

        /// <summary>
        /// This much time has to wait for the database to become available when too much
        /// different databases get loaded at the same time
        /// </summary>
        [Description("The time in seconds to wait for a database to start loading when under load")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.ConcurrentLoadTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ConcurrentLoadTimeout { get; set; }

        /// <summary>
        /// specifies the maximum amount of databases that can be loaded simultaneously
        /// </summary>
        [DefaultValue(8)]
        [ConfigurationEntry("Databases.MaxConcurrentLoads", ConfigurationEntryScope.ServerWideOnly)]
        public int MaxConcurrentLoads { get; set; }

        [DefaultValue(900)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.MaxIdleTimeInSec", ConfigurationEntryScope.ServerWideOnly)]
        [LegacyConfigurationEntry("Raven/Databases/MaxIdleTimeForTenantDatabase")]
        public TimeSetting MaxIdleTime { get; set; }

        [Description("The time in seconds to check for an idle database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.FrequencyToCheckForIdleInSec", ConfigurationEntryScope.ServerWideOnly)]
        [LegacyConfigurationEntry("Raven/Databases/FrequencyToCheckForIdleDatabases")]
        public TimeSetting FrequencyToCheckForIdle { get; set; }
    }
}
