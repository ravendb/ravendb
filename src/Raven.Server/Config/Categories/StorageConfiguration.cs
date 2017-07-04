using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class StorageConfiguration : ConfigurationCategory
    {
        [Description("You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Storage.TempPath")]
        [LegacyConfigurationEntry("Raven/Voron/TempPath")]
        public PathSetting TempPath { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Storage.TransactionJournalsPath")]
        [LegacyConfigurationEntry("Raven/TransactionJournalsPath")]
        public PathSetting JournalsStoragePath { get; set; }

        [Description("Use the 32 bits memory mapped pager, even when running in 64 bits")]
        [DefaultValue(false)]
        [ConfigurationEntry("Storage.ForceUsing32BitsPager", isServerWideOnly: true)]
        public bool ForceUsing32BitsPager { get; set; }

        [Description("How long transaction mode (Danger/Lazy) last before returning to Safe mode. Value in Minutes. Default one day. Zero for infinite time")]
        [DefaultValue(1440)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Storage.TransactionsModeDurationInMin")]
        public TimeSetting TransactionsModeDuration { get; set; }

        [Description("Maximum concurrent flushes")]
        [DefaultValue(10)]
        [ConfigurationEntry("Storage.MaxConcurrentFlushes")]
        public int MaxConcurrentFlushes { get; set; }

        [Description("Time to sync after flash in seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Storage.TimeToSyncAfterFlashInSec")]
        public TimeSetting TimeToSyncAfterFlash { get; set; }

        [Description("Number of concurrent syncs per physical drive")]
        [DefaultValue(3)]
        [ConfigurationEntry("Storage.NumberOfConcurrentSyncsPerPhysicalDrive")]
        public int NumberOfConcurrentSyncsPerPhysicalDrive { get; set; }

        [Description("Compress Transactions above size (value in KB)")]
        [DefaultValue(512)]
        [ConfigurationEntry("Storage.CompressTxAboveSizeInKB")]
        public int CompressTxAboveSizeInKb { get; set; }
    }
}