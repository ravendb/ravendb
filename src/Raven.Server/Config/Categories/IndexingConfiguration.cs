using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Utils;

namespace Raven.Server.Config.Categories
{
    public class IndexingConfiguration : ConfigurationCategory
    {
        private bool? _runInMemory;

        private readonly Func<string> _databaseName;
        private readonly Func<bool> _databaseRunInMemory;
        private readonly Func<string> _dataDirectory;

        private string _indexStoragePath;
        private string _tempPath;
        private string _journalsStoragePath;
        private string[] _additionalStoragePaths;

        public IndexingConfiguration(Func<string> databaseName, Func<bool> runInMemory, Func<string> dataDirectory) // TODO arek - maybe use Lazy instead
        {
            _databaseName = databaseName;
            _databaseRunInMemory = runInMemory;
            _dataDirectory = dataDirectory;
        }

        [Description("Whatever the indexes should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [IndexUpdateType(IndexUpdateType.Reset)]
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
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Raven/Indexing/Disable")]
        public virtual bool Disabled { get; protected set; }

        [Description("Default path for the indexes on disk. Useful if you want to store the indexes on another HDD for performance reasons.\r\nDefault: ~\\Databases\\[database-name]\\Indexes.")]
        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry(Constants.Configuration.Indexing.StoragePath)]
        [LegacyConfigurationEntry("Raven/IndexStoragePath")]
        public virtual string StoragePath
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
                {
                    _indexStoragePath = null;
                    return;
                }

                _indexStoragePath = AddDatabaseNameToPathIfNeeded(value.ToFullPath());
            }
        }

        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry(Constants.Configuration.Indexing.TempPath)]
        public virtual string TempPath
        {
            get
            {
                return _tempPath;
            }
            protected set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    _tempPath = null;
                    return;
                }

                _tempPath = AddDatabaseNameToPathIfNeeded(value.ToFullPath());
            }
        }

        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry(Constants.Configuration.Indexing.JournalsStoragePath)]
        public virtual string JournalsStoragePath
        {
            get
            {
                return _journalsStoragePath;
            }
            protected set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    _journalsStoragePath = null;
                    return;
                }

                _journalsStoragePath = AddDatabaseNameToPathIfNeeded(value.ToFullPath());
            }
        }

        [Description("List of paths separated by semicolon ';' where database will look for index when it loads.")]
        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry(Constants.Configuration.Indexing.AdditionalStoragePaths)]
        public virtual string[] AdditionalStoragePaths
        {
            get
            {
                return _additionalStoragePaths;
            }

            protected set
            {
                if (value == null)
                {
                    _additionalStoragePaths = null;
                    return;
                }

                _additionalStoragePaths = value
                    .Select(x => AddDatabaseNameToPathIfNeeded(x.ToFullPath()))
                    .ToArray();
            }
        }

        [Description("How long indexing will keep document transaction open when indexing. After this the transaction will be reopened.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Raven/Indexing/MaxTimeForDocumentTransactionToRemainOpenInSec")]
        public TimeSetting MaxTimeForDocumentTransactionToRemainOpen { get; protected set; }

        [Description("How long the database should wait before marking an auto index with the idle flag")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; protected set; }

        [Description("How long the database should wait before deleting an auto index with the idle flag")]
        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeDeletingAutoIndexMarkedAsIdleInHrs")]
        public TimeSetting TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle { get; protected set; }

        [Description("Limits the number of map outputs that a map index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 15. In order to disable this check set value to -1.")]
        [DefaultValue(15)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry(Constants.Configuration.Indexing.MaxMapIndexOutputsPerDocument)]
        public int MaxMapIndexOutputsPerDocument { get; protected set; }

        [Description("Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and the appropriate error message will be added to the indexing errors. Default value: 50. In order to disable this check set value to -1.")]
        [DefaultValue(50)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry(Constants.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument)]
        public int MaxMapReduceIndexOutputsPerDocument { get; protected set; }

        [Description("EXPERT ONLY")]
        [DefaultValue(16)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Raven/Indexing/MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory")]
        public int MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory { get; protected set; }

        [Description("Number of seconds after which mapping will end even if there is more to map. By default we will map everything we can in single batch.")]
        [DefaultValue(-1)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Raven/Indexing/MapTimeoutInSec")]
        public TimeSetting MapTimeout { get; protected set; }

        [Description("Number of minutes after which mapping will end even if there is more to map. This will only be applied if we pass the last etag in collection that we saw when batch was started.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Raven/Indexing/MapTimeoutAfterEtagReachedInMin")]
        public TimeSetting MapTimeoutAfterEtagReached { get; protected set; }

        protected string AddDatabaseNameToPathIfNeeded(string path)
        {
            var databaseName = _databaseName();
            if (string.IsNullOrWhiteSpace(databaseName))
                return path;

            return Path.Combine(path, databaseName);
        }

        protected override void ValidateProperty(PropertyInfo property)
        {
            var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();
            if (updateTypeAttribute == null)
                throw new InvalidOperationException($"No {nameof(IndexUpdateTypeAttribute)} available for '{property.Name}' property.");
        }
    }
}