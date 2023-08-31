using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.LowMemory;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Database)]
    public sealed class DatabaseConfiguration : ConfigurationCategory
    {
        public DatabaseConfiguration(bool forceUsing32BitsPager)
        {
            var totalMem = MemoryInformation.TotalPhysicalMemory;

            Size defaultPulseReadTransactionLimit;

            if (PlatformDetails.Is32Bits || forceUsing32BitsPager || totalMem <= new Size(1, SizeUnit.Gigabytes))
                defaultPulseReadTransactionLimit = new Size(16, SizeUnit.Megabytes);
            else if (totalMem <= new Size(4, SizeUnit.Gigabytes))
                defaultPulseReadTransactionLimit = new Size(32, SizeUnit.Megabytes);
            else if (totalMem <= new Size(16, SizeUnit.Gigabytes))
                defaultPulseReadTransactionLimit = new Size(64, SizeUnit.Megabytes);
            else if (totalMem <= new Size(64, SizeUnit.Gigabytes))
                defaultPulseReadTransactionLimit = new Size(128, SizeUnit.Megabytes);
            else
                defaultPulseReadTransactionLimit = new Size(256, SizeUnit.Megabytes);

            PulseReadTransactionLimit = defaultPulseReadTransactionLimit;
        }


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
        /// Whether revisions compression should be on by default or not.
        /// It is useful to turn this option off if you are expected to run on very low end hardware.
        /// That does not prevent you from enabling this after a database is created, only sets the default.
        /// </summary>
        [Description("Whether revisions compression should be on by default or not on new databases")]
        [DefaultValue(true)]
        [ConfigurationEntry("Databases.Compression.CompressRevisionsDefault", ConfigurationEntryScope.ServerWideOnly)]
        public bool CompressRevisionsDefault { get; set; }

        /// <summary>
        /// Whether collections compression should be on by default or not.
        /// This does not prevent you from enabling this after a database is created, only sets the default.
        /// </summary>
        [Description("Whether collections compression should be on by default or not on new databases")]
        [DefaultValue(false)]
        [ConfigurationEntry("Databases.Compression.CompressAllCollectionsDefault", ConfigurationEntryScope.ServerWideOnly)]
        public bool CompressAllCollectionsDefault { get; set; }

        /// <summary>
        /// Time to wait for the database to become available when too many different databases get loaded at the same time.
        /// </summary>
        [Description("The time in seconds to wait for a database to start loading when under load")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.ConcurrentLoadTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ConcurrentLoadTimeout { get; set; }

        /// <summary>
        /// Specifies the maximum number of databases that can be loaded simultaneously
        /// </summary>
        [Description("Specifies the maximum number of databases that can be loaded simultaneously")]
        [DefaultValue(8)]
        [ConfigurationEntry("Databases.MaxConcurrentLoads", ConfigurationEntryScope.ServerWideOnly)]
        public int MaxConcurrentLoads { get; set; }

        /// <summary>
        /// Set time in seconds for maximum idle time for the database.  
        /// After this time an idle database will be unloaded from memory.  
        /// </summary>
        [Description("Specifies the maximum idle time for the database. After this time an idle database will be unloaded from memeory.")]
        [DefaultValue(900)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.MaxIdleTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxIdleTime { get; set; }

        /// <summary>
        /// The time in seconds to check for an idle database
        /// </summary>
        [Description("The time in seconds to check for an idle database")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Databases.FrequencyToCheckForIdleInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting FrequencyToCheckForIdle { get; set; }

        /// <summary>
        /// Number of megabytes occupied by encryption buffers (if database is encrypted) or mapped 32 bites buffers (when running on 32 bits)
        /// after which a read transaction will be renewed to reduce memory usage during long running operations like backups or streaming.
        /// </summary>
        [Description("Number of megabytes occupied by encryption buffers (if database is encrypted) or mapped 32 bites buffers (when running on 32 bits) after which a read transaction will be renewed to reduce memory usage during long running operations like backups or streaming")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Databases.PulseReadTransactionLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size PulseReadTransactionLimit { get; set; }

        /// <summary>
        /// EXPERT: A deep database cleanup will be done when this number of minutes has passed since the last time work was done on the database.
        /// </summary>
        [Description("EXPERT: A deep database cleanup will be done when this number of minutes has passed since the last time work was done on the database.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Databases.DeepCleanupThresholdInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting DeepCleanupThreshold { get; set; }

        /// <summary>
        /// EXPERT: A regular database cleanup will be done when this number of minutes has passed since the last database idle time.
        /// </summary>
        [Description("EXPERT: A regular database cleanup will be done when this number of minutes has passed since the last database idle time.")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Databases.RegularCleanupThresholdInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RegularCleanupThreshold { get; set; }
    }
}
