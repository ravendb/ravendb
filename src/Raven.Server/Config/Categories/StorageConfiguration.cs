using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public class StorageConfiguration : ConfigurationCategory
    {
        [Description("You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Storage.TempPath", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public PathSetting TempPath { get; set; }

        [Description("Use the 32 bits memory mapped pager, even when running in 64 bits")]
        [DefaultValue(false)]
        [ConfigurationEntry("Storage.ForceUsing32BitsPager", ConfigurationEntryScope.ServerWideOnly)]
        public bool ForceUsing32BitsPager { get; set; }

        [Description("How long transaction mode (Danger/Lazy) last before returning to Safe mode. Value in Minutes. Default one day. Zero for infinite time")]
        [DefaultValue(1440)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Storage.TransactionsModeDurationInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting TransactionsModeDuration { get; set; }

        [Description("Maximum concurrent flushes")]
        [DefaultValue(10)]
        [ConfigurationEntry("Storage.MaxConcurrentFlushes", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxConcurrentFlushes { get; set; }

        [Description("Time to sync after flash in seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Storage.TimeToSyncAfterFlashInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting TimeToSyncAfterFlash { get; set; }

        [Description("Number of concurrent syncs per physical drive")]
        [DefaultValue(3)]
        [ConfigurationEntry("Storage.NumberOfConcurrentSyncsPerPhysicalDrive", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int NumberOfConcurrentSyncsPerPhysicalDrive { get; set; }

        [Description("Compress transactions above size (value in KB)")]
        [DefaultValue(512)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Storage.CompressTxAboveSizeInKb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size CompressTxAboveSize { get; set; }

        [Description("Max size of .buffers files")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Storage.MaxScratchBufferSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? MaxScratchBufferSize { get; set; }

        [Description("EXPERT: Allows to load a database regardless journal errors that can be thrown during the recovery operation on startup. Since journals are mandatory to properly start a database, the usage of this option is dangerous")]
        [DefaultValue(null)]
        [ConfigurationEntry("Storage.Dangerous.IgnoreInvalidJournalErrors", ConfigurationEntryScope.ServerWideOnly)]
        public bool? IgnoreInvalidJournalErrors { get; set; }
    }
}
