using System;
using System.ComponentModel;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class IndexingConfiguration : ConfigurationCategory
    {
        private bool? _runInMemory;

        private readonly Func<bool> _databaseRunInMemory;
        private readonly Func<string> _dataDirectory;

        private string _indexStoragePath;

        public IndexingConfiguration(Func<bool> runInMemory, Func<string> dataDirectory) // TODO arek - maybe use Lazy instead
        {
            _databaseRunInMemory = runInMemory;
            _dataDirectory = dataDirectory;
        }

        [Description("Whatever the indexes should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [ConfigurationEntry("Raven/Indexing/RunInMemory", setDefaultValueIfNeeded: false)]
        public virtual bool RunInMemory
        {
            get
            {
                if (_runInMemory == null)
                    _runInMemory = _databaseRunInMemory();

                return _runInMemory.Value;
            }

            protected set { _runInMemory = value; }
        }

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/Disable")]
        public virtual bool Disabled { get; protected set; }

        [Description("Default path for the indexes on disk. Useful if you want to store the indexes on another HDD for performance reasons.\r\nDefault: ~\\Databases\\[database-name]\\Indexes.")]
        [DefaultValue(null)]
        [ConfigurationEntry(Constants.Configuration.Indexing.StoragePath)]
        [LegacyConfigurationEntry("Raven/IndexStoragePath")]
        public virtual string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(_indexStoragePath))
                    _indexStoragePath = Path.Combine(_dataDirectory(), "Indexes");
                return _indexStoragePath;
            }
            protected set
            {
                if (string.IsNullOrWhiteSpace(value))
                    return;
                _indexStoragePath = value.ToFullPath();
            }
        }

        [Description("List of paths separated by semicolon ';' where database will look for index when it loads.")]
        [DefaultValue(null)]
        [ConfigurationEntry(Constants.Configuration.Indexing.AdditionalIndexStoragePaths)]
        public string[] AdditionalIndexStoragePaths { get; protected set; }

        [Description("How long indexing will keep document transaction open when indexing. After this the transaction will be reopened.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/MaxTimeForDocumentTransactionToRemainOpenInSec")]
        public TimeSetting MaxTimeForDocumentTransactionToRemainOpen { get; protected set; }

        [Description("How long the database should wait before marking an auto index with the idle flag")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; protected set; }

        [Description("How long the database should wait before deleting an auto index with the idle flag")]
        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeDeletingAutoIndexMarkedAsIdleInHrs")]
        public TimeSetting TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle { get; protected set; }

        [Description("Limits the number of map outputs that a map index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 15. In order to disable this check set value to -1.")]
        [DefaultValue(15)]
        [ConfigurationEntry(Constants.Configuration.Indexing.MaxMapIndexOutputsPerDocument)]
        public int MaxMapIndexOutputsPerDocument { get; protected set; }

        [Description("Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 50. In order to disable this check set value to -1.")]
        [DefaultValue(50)]
        [ConfigurationEntry(Constants.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument)]
        public int MaxMapReduceIndexOutputsPerDocument { get; protected set; }

        [Description("EXPERT ONLY")]
        [DefaultValue(16)]
        [ConfigurationEntry("Raven/Indexing/MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory")]
        public int MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory { get; protected set; }

        [Description("Number of seconds after which mapping will end even if there is more to map. By default we will map everything we can in single batch.")]
        [DefaultValue(-1)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Indexing/MapTimeoutInSec")]
        public TimeSetting MapTimeout { get; protected set; }

        [Description("Number of minutes after which mapping will end even if there is more to map. This will only be applied if we pass the last etag in collection that we saw when batch was started.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/MapTimeoutAfterEtagReachedInMin")]
        public TimeSetting MapTimeoutAfterEtagReached { get; protected set; }
    }
}