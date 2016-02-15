using System;
using System.ComponentModel;
using System.IO;
using Raven.Abstractions.Extensions;
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

        [Description("Maximum number of items to map by index during single indexing run")]
        [DefaultValue(128 * 1024)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfItemsToFetchForMap")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
        public int MaxNumberOfItemsToFetchForMap { get; set; }

        [Description("Maximum number of items to reduce by index during single indexing run")]
        [DefaultValue(64 * 1024)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfItemsToFetchForReduce")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
        public int MaxNumberOfItemsToFetchForReduce { get; set; }

        [Description("Number of seconds after which index will stop reading documents from disk")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/FetchingDocumentsFromDiskTimeoutInSec")]
        [ConfigurationEntry("Raven/Prefetcher/FetchingDocumentsFromDiskTimeout")]
        public TimeSetting FetchingDocumentsFromDiskTimeout { get; set; }

        [Description("Maximum number of megabytes after which index will stop reading documents from disk")]
        [DefaultValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Indexing/MaximumSizeAllowedToFetchFromStorageInMB")]
        [ConfigurationEntry("Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage")]
        public Size MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
    }
}