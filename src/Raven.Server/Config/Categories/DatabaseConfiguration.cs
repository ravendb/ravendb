using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Config.Categories
{
    public class DatabaseConfiguration : ConfigurationCategory
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
        /// This much time has to wait for the database to become available when too much
        /// different databases get loaded at the same time
        /// </summary>
        [Description("The time in seconds to wait for a database to start loading when under load")]
        [DefaultValue(60)]
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

        [Description("Number of megabytes occupied by encryption buffers (if database is encrypted) or mapped 32 bites buffers (when running on 32 bits) after which a read transaction will be renewed to reduce memory usage during long running operations like backups or streaming")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Databases.PulseReadTransactionLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size PulseReadTransactionLimit { get; set; }
    }
}
