using System;
using System.ComponentModel;
using System.IO;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class IndexingConfiguration : ConfigurationCategory
    {
        private readonly Func<bool> _runInMemory;
        private readonly Func<string> _dataDirectory;

        private string _indexStoragePath;

        public IndexingConfiguration(Func<bool> runInMemory, Func<string> dataDirectory) // TODO arek - maybe use Lazy instead
        {
            _runInMemory = runInMemory;
            _dataDirectory = dataDirectory;
        }

        public bool RunInMemory => _runInMemory();

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/Disable")]
        public bool Disabled { get; set; }

        [Description("The path for the indexes on disk. Useful if you want to store the indexes on another HDD for performance reasons.\r\nDefault: ~\\Databases\\[database-name]\\Indexes.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/Indexing/StoragePath")]
        [LegacyConfigurationEntry("Raven/IndexStoragePath")]
        public string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(_indexStoragePath))
                    _indexStoragePath = Path.Combine(_dataDirectory(), "Indexes");
                return _indexStoragePath;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    return;
                _indexStoragePath = value.ToFullPath();
            }
        }

        [Description("How long indexing will keep document transaction open when indexing. After this the transaction will be reopened.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/MaxTimeForDocumentTransactionToRemainOpenInSec")]
        public TimeSetting MaxTimeForDocumentTransactionToRemainOpenInSec { get; set; }

        [Description("How long the database should wait before marking an auto index with the idle flag")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; set; }

        [Description("How long the database should wait before deleting an auto index with the idle flag")]
        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeDeletingAutoIndexMarkedAsIdleInHrs")]
        public TimeSetting TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle { get; set; }

        [Description("Limits the number of map outputs that a map index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 15. In order to disable this check set value to -1.")]
        [DefaultValue(15)]
        [ConfigurationEntry("Raven/Indexing/MaxMapIndexOutputsPerDocument")]
        public int MaxMapIndexOutputsPerDocument { get; set; }

        [Description("Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 50. In order to disable this check set value to -1.")]
        [DefaultValue(50)]
        [ConfigurationEntry("Raven/Indexing/MaxMapIndexOutputsPerDocument")]
        public int MaxMapReduceIndexOutputsPerDocument { get; set; }
    }
}