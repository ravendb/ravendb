using System;
using System.ComponentModel;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Indexing)]
    public class IndexingConfiguration : ConfigurationCategory
    {
        private bool? _runInMemory;

        private readonly RavenConfiguration _root;

        private PathSetting _indexStoragePath;

        public IndexingConfiguration(RavenConfiguration root)
        {
            _root = root;

            MaximumSizePerSegment = new Size(PlatformDetails.Is32Bits ? 128 : 1024, SizeUnit.Megabytes);
            LargeSegmentSizeToMerge = new Size(PlatformDetails.Is32Bits ? 16 : 32, SizeUnit.Megabytes);
        }

        [DefaultValue(false)]
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

        [DefaultValue(true)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.Metrics.Enabled", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public virtual bool EnableMetrics { get; protected set; }

        [ReadOnlyPath]
        public virtual PathSetting StoragePath => _indexStoragePath ??= _root.ResourceType == ResourceType.Server ? null : _root.Core.DataDirectory.Combine("Indexes");

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

        [Description("Maximum number of mapped documents. Cannot be less than 128. By default 'null' - no limit.")]
        [DefaultValue(null)]
        [MinValue(128)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MapBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MapBatchSize { get; protected set; }

        [Description("Number of minutes after which mapping will end even if there is more to map. This will only be applied if we pass the last etag in collection that we saw when batch was started.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MapTimeoutAfterEtagReachedInMin", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MapTimeoutAfterEtagReached { get; protected set; }

        [Description("Max number of steps in the script execution of a JavaScript index")]
        [DefaultValue(10_000)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.MaxStepsForScript", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxStepsForScript { get; set; }

        [Description("Time (in minutes) between index cleanup")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.CleanupIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CleanupInterval { get; set; }

        [Description("Smallest n-gram to generate when NGram analyzer is used")]
        [DefaultValue(2)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.Analyzers.NGram.MinGram", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MinGram { get; set; }

        [Description("Largest n-gram to generate when NGram analyzer is used")]
        [DefaultValue(6)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.Analyzers.NGram.MaxGram", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxGram { get; set; }

        [Description("Managed allocations limit in an indexing batch after which the batch will complete and an index will continue by starting a new one")]
        [DefaultValue(2048)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.ManagedAllocationsBatchSizeLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? ManagedAllocationsBatchLimit { get; protected set; }

        [Description("Expert: The maximum size in MB that we'll consider for segments merging")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MaximumSizePerSegmentInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size MaximumSizePerSegment { get; protected set; }

        [Description("Expert: How often segment indices are merged. With smaller values, less RAM is used while indexing, and searches on unoptimized indices are faster, but indexing speed is slower")]
        [DefaultValue(10)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MergeFactor", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MergeFactor { get; protected set; }

        [Description("Expert: The definition of a large segment in MB. We wont merge more than " + nameof(NumberOfLargeSegmentsToMergeInSingleBatch) + " in a single batch")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.LargeSegmentSizeToMergeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size LargeSegmentSizeToMerge { get; protected set; }

        [Description("Expert: Number of large segments (defined by " + nameof(LargeSegmentSizeToMerge) + ") to merge in a single batch")]
        [DefaultValue(2)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.NumberOfLargeSegmentsToMergeInSingleBatch", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int NumberOfLargeSegmentsToMergeInSingleBatch { get; protected set; }

        [Description("Expert: How long will we let merges to run before we close the transaction")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.MaxTimeForMergesToKeepRunningInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeForMergesToKeepRunning { get; protected set; }

        [Description("Transaction size limit after which an index will stop and complete the current batch")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.TransactionSizeLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? TransactionSizeLimit { get; protected set; }

        [Description("Transaction size limit, for encrypted database only, after which an index will stop and complete the current batch")]
        [DefaultValue(96)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.Encrypted.TransactionSizeLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? EncryptedTransactionSizeLimit { get; protected set; }

        [Description("Amount of scratch space that we allow to use for the index storage. After exceeding this limit the current indexing batch will complete and the index will force flush and sync storage environment.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.ScratchSpaceLimitInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? ScratchSpaceLimit { get; protected set; }

        [Description("Maximum amount of scratch space that we allow to use for all index storages per server. After exceeding this limit the indexes will complete their current indexing batches and force flush and sync storage environments.")]
        [DefaultValue(null)]
        [SizeUnit(SizeUnit.Megabytes)]
        [IndexUpdateType(IndexUpdateType.Refresh)]
        [ConfigurationEntry("Indexing.GlobalScratchSpaceLimitInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? GlobalScratchSpaceLimit { get; protected set; }

        [Description("Max time to wait when forcing the storage environment flush and sync after exceeding scratch space limit")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.MaxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MaxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit { get; protected set; }

        [Description("Indicates if missing fields should be indexed same as 'null' values or not. Default: false")]
        [DefaultValue(false)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.IndexMissingFieldsAsNull", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool IndexMissingFieldsAsNull { get; set; }

        [Description("Indicates if empty index entries should be indexed by static indexes. Default: false")]
        [DefaultValue(false)]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.IndexEmptyEntries", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool IndexEmptyEntries { get; set; }

        [Description("Indicates how error indexes should behave on database startup when they are loaded. By default they are not started.")]
        [DefaultValue(IndexStartupBehavior.Default)]
        [IndexUpdateType(IndexUpdateType.None)]
        [ConfigurationEntry("Indexing.ErrorIndexStartupBehavior", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public IndexStartupBehavior ErrorIndexStartupBehavior { get; set; }

        [Description("Location of NuGet packages cache")]
        [DefaultValue("Packages/NuGet")]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.NuGetPackagesPath", ConfigurationEntryScope.ServerWideOnly)]
        public PathSetting NuGetPackagesPath { get; set; }

        [Description("Default NuGet source URL")]
        [DefaultValue("https://api.nuget.org/v3/index.json")]
        [IndexUpdateType(IndexUpdateType.Reset)]
        [ConfigurationEntry("Indexing.NuGetPackageSourceUrl", ConfigurationEntryScope.ServerWideOnly)]
        public string NuGetPackageSourceUrl { get; set; }

        protected override void ValidateProperty(PropertyInfo property)
        {
            base.ValidateProperty(property);

            var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();
            if (updateTypeAttribute == null)
                throw new InvalidOperationException($"No {nameof(IndexUpdateTypeAttribute)} available for '{property.Name}' property.");
        }

        public enum IndexStartupBehavior
        {
            Default,
            Start,
            ResetAndStart
        }
    }
}
