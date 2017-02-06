using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class StorageConfiguration : ConfigurationCategory
    {
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Storage/PreventSchemaUpdate")]
        [LegacyConfigurationEntry("Raven/PreventSchemaUpdate")]
        public bool PreventSchemaUpdate { get; set; }

        [Description("You can use this setting to specify a maximum buffer pool size that can be used for transactional storage")]
        [DefaultValue(4)]
        [MinValue(2)]
        [SizeUnit(SizeUnit.Gigabytes)]
        [ConfigurationEntry("Raven/Storage/MaxBufferPoolSizeInGB")]
        [LegacyConfigurationEntry("Raven/Voron/MaxBufferPoolSize")]
        public Size MaxBufferPoolSize { get; set; }

        [Description("You can use this setting to specify an initial file size for data file")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Raven/Storage/InitialFileSize")]
        [LegacyConfigurationEntry("Raven/Voron/InitialFileSize")]
        public Size? InitialFileSize { get; set; }

        [Description("The maximum scratch buffer (modified data by active transactions) size that can be used by Voron")]
        [DefaultValue(6144)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Storage/MaxScratchBufferSizeInMB")]
        [LegacyConfigurationEntry("Raven/Voron/MaxScratchBufferSize")]
        public Size MaxScratchBufferSize { get; set; }

        [Description("The minimum number of megabytes after which each scratch buffer size increase will create a notification. Used for indexing batch size tuning.\r\n" +
                     "Default: \r\n" +
                     "1024 when MaxScratchBufferSize > 1024,\r\n" +
                     "512 when MaxScratchBufferSize > 512\r\n" +
                     "null otherwise (disabled)")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Storage/ScratchBufferSizeNotificationThresholdInMB")]
        [LegacyConfigurationEntry("Raven/Voron/ScratchBufferSizeNotificationThreshold")]
        public Size? ScratchBufferSizeNotificationThreshold { get; set; }

        [Description("If you want to use incremental backups, you need to turn this to true, but then journal files will not be deleted after applying them to the data file. They will be deleted only after a successful backup.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Storage/AllowIncrementalBackups")]
        [LegacyConfigurationEntry("Raven/Voron/AllowIncrementalBackups")]
        public bool AllowIncrementalBackups { get; set; }

        [Description("You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Storage/TempPath")]
        [LegacyConfigurationEntry("Raven/Voron/TempPath")]
        public PathSetting TempPath { get; set; }

        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Storage/TransactionJournalsPath")]
        [LegacyConfigurationEntry("Raven/TransactionJournalsPath")]
        public PathSetting JournalsStoragePath { get; set; }

        // TODO: We always uses voron
        [Description("Whether to allow Voron to run in 32 bits process.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Storage/AllowOn32Bits")]
        [LegacyConfigurationEntry("Raven/Voron/AllowOn32Bits")]
        public bool AllowOn32Bits { get; set; }

        [Description("How long transaction mode (Danger/Lazy) last before returning to Safe mode. Value in Minutes. Default one day. Zero for infinite time")]
        [DefaultValue(1440)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Storage/TransactionsModeDuration")]
        [LegacyConfigurationEntry("Raven/TransactionsModeDuration")]
        public int TransactionsModeDuration { get; set; }

        [Description("Maximum concurrent flushes")]
        [DefaultValue(10)]
        [ConfigurationEntry("Raven/Storage/MaxConcurrentFlushes")]
        public int MaxConcurrentFlushes { get; set; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);

            if (ScratchBufferSizeNotificationThreshold == null)
            {
                var _1024MB = new Size(1024, SizeUnit.Megabytes);
                var _512MB = new Size(512, SizeUnit.Megabytes);

                if (MaxScratchBufferSize > _1024MB)
                    ScratchBufferSizeNotificationThreshold = _1024MB;
                else if (MaxScratchBufferSize > _512MB)
                    ScratchBufferSizeNotificationThreshold = _512MB;
            }
        }
    }
}