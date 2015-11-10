using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;
using Raven.Database.FileSystem.Util;

namespace Raven.Database.Config.Categories
{
    public class TimeSeriesConfiguration : ConfigurationCategory
    {
        private readonly CoreConfiguration coreConfiguration;

        public TimeSeriesConfiguration(CoreConfiguration coreConfiguration)
        {
            this.coreConfiguration = coreConfiguration;
        }

        private string timeSeriesDataDirectory;

        /// <summary>
        /// The directory for the RavenDB time series. 
        /// You can use the ~\ prefix to refer to RavenDB's base directory. 
        /// </summary>
        [DefaultValue(@"~\TimeSeries")]
        [ConfigurationEntry("Raven/TimeSeries/DataDir")]
        public string DataDirectory
        {
            get { return timeSeriesDataDirectory; }
            set { timeSeriesDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(coreConfiguration.WorkingDirectory, value); }
        }

        /// <summary>
        /// Determines how long tombstones will be kept by a time series. After the specified time they will be automatically
        /// Purged on next time series startup. Default: 14 days.
        /// </summary>
        [DefaultValue(14)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Raven/TimeSeries/TombstoneRetentionTimeInDays")]
        [ConfigurationEntry("Raven/TimeSeries/TombstoneRetentionTime")]
        public TimeSetting TombstoneRetentionTime { get; set; }

        [DefaultValue(1000)]
        [ConfigurationEntry("Raven/TimeSeries/DeletedTombstonesInBatch")]
        public int DeletedTombstonesInBatch { get; set; }

        [DefaultValue(30 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/TimeSeries/ReplicationLatency")]
        public TimeSetting ReplicationLatency { get; set; }
    }
}