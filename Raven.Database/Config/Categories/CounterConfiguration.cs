using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;
using Raven.Database.FileSystem.Util;

namespace Raven.Database.Config.Categories
{
    public class CounterConfiguration : ConfigurationCategory
    {
        private readonly CoreConfiguration coreConfiguration;

        public CounterConfiguration(CoreConfiguration coreConfiguration)
        {
            this.coreConfiguration = coreConfiguration;
        }


        private string countersDataDirectory;

        /// <summary>
        /// The directory for the RavenDB counters. 
        /// You can use the ~\ prefix to refer to RavenDB's base directory. 
        /// </summary>
        [DefaultValue(@"~\Counters")]
        [ConfigurationEntry("Raven/Counter/DataDir")]
        [ConfigurationEntry("Raven/Counters/DataDir")]
        public string DataDirectory
        {
            get { return countersDataDirectory; }
            set { countersDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(coreConfiguration.WorkingDirectory, value); }
        }

        /// <summary>
        /// Determines how long tombstones will be kept by a counter storage. After the specified time they will be automatically
        /// Purged on next counter storage startup. Default: 14 days.
        /// </summary>
        [DefaultValue(14)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Raven/Counter/TombstoneRetentionTimeInDays")]
        [ConfigurationEntry("Raven/Counter/TombstoneRetentionTime")]
        public TimeSetting TombstoneRetentionTime { get; set; }

        [DefaultValue(1000)]
        [ConfigurationEntry("Raven/Counter/DeletedTombstonesInBatch")]
        public int DeletedTombstonesInBatch { get; set; }

        [DefaultValue(30 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Counter/ReplicationLatency")]
        public TimeSetting ReplicationLatency { get; set; }
    }
}