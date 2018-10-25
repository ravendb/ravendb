using System;
using System.ComponentModel;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.ServerWide;
using Sparrow;

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

        [Description("Whether the indexes should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.RunInMemory", ConfigurationEntryScope.ServerWideOrPerDatabase, setDefaultValueIfNeeded: false)]
        public virtual bool RunInMemory
        {
            get
            {
                if (_runInMemory == null)
                    _runInMemory = _root.Core.RunInMemory;

                return _runInMemory.Value;
            }

            protected set => _runInMemory = value;
        }

        [DefaultValue(false)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.Disable", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public virtual bool Disabled { get; protected set; }

        [ReadOnlyPath]
        public virtual PathSetting StoragePath => _indexStoragePath ?? (_indexStoragePath = _root.ResourceType == ResourceType.Server ? null : _root.Core.DataDirectory.Combine("Indexes"));

        [DefaultValue(null)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.TempPath", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public virtual PathSetting TempPath { get; protected set; }

        [Description("How long indexing will keep document transaction open when indexing. After this the transaction will be reopened.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MaxTimeForDocumentTransactionToRemainOpenInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeForDocumentTransactionToRemainOpen { get; protected set; }

        [Description("How long should we keep a superseded auto index?")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.TimeBeforeDeletionOfSupersededAutoIndexInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting TimeBeforeDeletionOfSupersededAutoIndex { get; protected set; }

        [Description("How long the database should wait before marking an auto index with the idle flag")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdleInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; protected set; }

        [Description("EXPERT ONLY")]
        [DefaultValue(false)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.DisableQueryOptimizerGeneratedIndexes", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool DisableQueryOptimizerGeneratedIndexes { get; protected set; }

        [Description("How long the database should wait before deleting an auto index with the idle flag")]
        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdleInHrs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle { get; protected set; }

        [Description("EXPERT ONLY")]
        [DefaultValue(512)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory { get; protected set; }

        [Description("EXPERT ONLY")]
        [DefaultValue(2)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.NumberOfConcurrentStoppedBatchesIfRunningLowOnMemory", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int NumberOfConcurrentStoppedBatchesIfRunningLowOnMemory { get; protected set; }

        [Description("Number of seconds after which mapping will end even if there is more to map. By default we will map everything we can in single batch.")]
        [DefaultValue(-1)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MapTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MapTimeout { get; protected set; }

        [Description("Number of minutes after which mapping will end even if there is more to map. This will only be applied if we pass the last etag in collection that we saw when batch was started.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MapTimeoutAfterEtagReachedInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MapTimeoutAfterEtagReached { get; protected set; }

        [Description("Max number of steps in the script execution of a JavaScript index")]
        [DefaultValue(10_000)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MaxStepsForScript", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxStepsForScript { get; set; }

        [Description("Time (in minutes) between index cleanup")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.CleanupIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CleanupInterval { get; set; }

        [Description("Maximum amount of scratch space that we allow to use for an indexing batch")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.ScratchSpaceLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? ScratchSpaceLimit { get; protected set; }

        [Description("Maximum amount of scratch space that we allow to use for entire indexing work per server")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.GlobalScratchSpaceLimitInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? GlobalScratchSpaceLimit { get; protected set; }

        [Description("Max time to wait when forcing the environment flush and sync after exceeding global scratch buffer limit")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.MaxTimeToWaitAfterFlushAndSyncWhenExceedingGlobalScratchSpaceLimit", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeToWaitAfterFlushAndSyncWhenExceedingGlobalScratchSpaceLimit { get; protected set; }

        protected override void ValidateProperty(PropertyInfo property)
        {
            var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();
            if (updateTypeAttribute == null)
                throw new InvalidOperationException($"No {nameof(IndexUpdateTypeAttribute)} available for '{property.Name}' property.");
        }
    }
}
