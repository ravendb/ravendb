using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Global;

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

        [Description("Size of the batch that will be requested to the OS from disk when prefetching (value in powers of 2). Some OSs may not honor certain values. Experts only.")]
        [DefaultValue(1024)]
        [SizeUnit(SizeUnit.Kilobytes)]
        [ConfigurationEntry("Storage.PrefetchBatchSizeInKb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size PrefetchBatchSize { get; set; }

        [Description("How many gigabytes of memory should be prefetched before restarting the prefetch tracker table. Experts only.")]
        [DefaultValue(8)]
        [SizeUnit(SizeUnit.Gigabytes)]
        [ConfigurationEntry("Storage.PrefetchResetThresholdInGb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size PrefetchResetThreshold { get; set; }

        [Description("Number of Journals files for each storage, before forcing a sync and removing unused journal files.")]
        [DefaultValue(2)]
        [ConfigurationEntry("Storage.SyncJournalsCountThreshold", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int SyncJournalsCountThreshold { get; set; }

        [Description("EXPERT: A command or executable to run when creating/opening a directory (storage environment). RavenDB will execute: command [user-arg-1] ... [user-arg-n] <environment-type> <database-name> <data-dir-path> <temp-dir-path> <journal-dir-path>")]
        [DefaultValue(null)]
        [ConfigurationEntry("Storage.OnCreateDirectory.Exec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [ConfigurationEntry("Storage.OnDirectoryInitialize.Exec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public string OnDirectoryInitializeExec { get; set; }

        [Description("EXPERT: The optional user arguments for the 'Storage.OnDirectoryInitialize.Exec' command or executable. The arguments must be escaped for the command line.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Storage.OnCreateDirectory.Exec.Arguments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [ConfigurationEntry("Storage.OnDirectoryInitialize.Exec.Arguments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public string OnDirectoryInitializeExecArguments { get; set; }

        [Description("EXPERT: The number of seconds to wait for the OnDirectoryInitialize executable to exit. Default: 30 seconds")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Storage.OnCreateDirectory.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        [ConfigurationEntry("Storage.OnDirectoryInitialize.Exec.TimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting OnDirectoryInitializeExecTimeout { get; set; }
    }
}
