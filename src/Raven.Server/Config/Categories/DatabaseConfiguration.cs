using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class DatabaseConfiguration : ConfigurationCategory
    {
        /// <summary>
        /// The time in seconds to wait before canceling query
        /// </summary>
        [Description("The time in seconds to wait before canceling query")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.QueryTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting QueryTimeout { get; set; }

        /// <summary>
        /// The time in seconds to wait before canceling query related operation (patch/delete query)
        /// </summary>
        [Description("The time in seconds to wait before canceling query related operation (patch/delete query)")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.QueryOperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting QueryOperationTimeout { get; set; }

        /// <summary>
        /// The time in seconds to wait before canceling specific operations (such as indexing terms)
        /// </summary>
        [Description("The time in seconds to wait before canceling specific operations (such as indexing terms)")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.OperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting OperationTimeout { get; set; }

        /// <summary>
        /// The time in seconds to wait before canceling several collection operations (such as batch delete documents)
        /// </summary>
        [Description("The time in seconds to wait before canceling several collection operations (such as batch delete documents from studio)")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.CollectionOperationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting CollectionOperationTimeout { get; set; }

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
        public TimeSetting MaxIdleTime { get; set; }

        [Description("The time in seconds to check for an idle database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.FrequencyToCheckForIdleInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting FrequencyToCheckForIdle { get; set; }
    }
}
