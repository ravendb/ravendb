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
        [ConfigurationEntry("Raven/IndexStoragePath")]
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

        [Description("Maximum number of documents to map by index during single indexing run")]
        [DefaultValue(128 * 1024)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfDocumentsToFetchForMap")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
        public int MaxNumberOfDocumentsToFetchForMap { get; set; }

        [Description("Maximum number of documents to reduce by index during single indexing run")]
        [DefaultValue(64 * 1024)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfDocumentsToFetchForReduce")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
        public int MaxNumberOfDocumentsToFetchForReduce { get; set; }

        [Description("Maximum number of tombstones to process by index during single indexing run")]
        [DefaultValue(16 * 1024)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfTombstonesToFetch")]
        public int MaxNumberOfTombstonesToFetch { get; set; }

        [Description("Number of seconds after which index will stop reading documents from disk and writing documents to index")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/DocumentProcessingTimeout")]
        [ConfigurationEntry("Raven/Prefetcher/FetchingDocumentsFromDiskTimeout")]
        public TimeSetting DocumentProcessingTimeout { get; set; }

        [Description("Number of seconds after which index will stop reading tombstones from disk and writing deletes to index")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/TombstoneProcessingTimeout")]
        public TimeSetting TombstoneProcessingTimeout { get; set; }

        [Description("How long the database should wait before marking an auto index with the idle flag")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; set; }

        [Description("How long the database should wait before marking an auto index with the abandoned flag")]
        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsAbandonedInHrs")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsAbandoned { get; set; }
    }
}