using System;
using System.ComponentModel;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Configuration;

namespace Raven.Server.Config.Categories
{
    public class IndexingConfiguration : ConfigurationCategory
    {
        private bool? _runInMemory;
        
        private readonly RavenConfiguration _root;

        private PathSetting _indexStoragePath;

        public IndexingConfiguration(RavenConfiguration root)
        {
            _root = root;
        }

        [Description("Whatever the indexes should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Raven/Indexing/RunInMemory", setDefaultValueIfNeeded: false)]
        public virtual bool RunInMemory
        {
            get
            {
                if (_runInMemory == null)
                    _runInMemory = _root.Core.RunInMemory;

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
        [ConfigurationEntry("Raven/Indexing/StoragePath")]
        [LegacyConfigurationEntry("Raven/IndexStoragePath")]
        public virtual PathSetting StoragePath
        {
            get
            {
                return _indexStoragePath ?? (_indexStoragePath = _root.Core.DataDirectory.Combine("Indexes"));
            }
            protected set
            {
                _indexStoragePath = value;
            }
        }

        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Raven/Indexing/TempPath")]
        public virtual PathSetting TempPath { get; protected set; }

        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Raven/Indexing/JournalsStoragePath")]
        public virtual PathSetting JournalsStoragePath { get; protected set; }

        [Description("List of paths separated by semicolon ';' where database will look for index when it loads.")]
        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Raven/Indexing/AdditionalStoragePaths")]
        public virtual PathSetting[] AdditionalStoragePaths { get; protected set; }

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

        protected override void ValidateProperty(PropertyInfo property)
        {
            var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();
            if (updateTypeAttribute == null)
                throw new InvalidOperationException($"No {nameof(IndexUpdateTypeAttribute)} available for '{property.Name}' property.");
        }
    }
}