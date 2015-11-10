using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;
using Raven.Database.Indexing;

namespace Raven.Database.Config.Categories
{
    public class IndexingConfiguration : ConfigurationCategory
    {
        private bool useLuceneASTParser = true;

        [DefaultValue(256 * 1024)]
        [ConfigurationEntry("Raven/Indexing/MaxWritesBeforeRecreate")]
        [ConfigurationEntry("Raven/MaxIndexWritesBeforeRecreate")]
        public int MaxWritesBeforeRecreate { get; set; }

        /// <summary>
        /// Limits the number of map outputs that a simple index is allowed to create for a one source document. If a map operation applied to the one document
        /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
        /// the appropriate error message will be added to the indexing errors.
        /// Default value: 15. In order to disable this check set value to -1.
        /// </summary>
        [DefaultValue(15)]
        [ConfigurationEntry("Raven/Indexing/MaxSimpleIndexOutputsPerDocument")]
        [ConfigurationEntry("Raven/MaxSimpleIndexOutputsPerDocument")]
        public int MaxSimpleIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document
        /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
        /// the appropriate error message will be added to the indexing errors.
        /// Default value: 50. In order to disable this check set value to -1.
        /// </summary>
        [DefaultValue(50)]
        [ConfigurationEntry("Raven/Indexing/MaxMapReduceIndexOutputsPerDocument")]
        [ConfigurationEntry("Raven/MaxMapReduceIndexOutputsPerDocument")]
        public int MaxMapReduceIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// How long can we keep the new index in memory before we have to flush it
        /// </summary>
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/NewIndexInMemoryMaxTimeInMin")]
        [ConfigurationEntry("Raven/NewIndexInMemoryMaxTime")]
        public TimeSetting NewIndexInMemoryMaxTime { get; set; }

        /// <summary>
        /// New indexes are kept in memory until they reach this integer value in bytes or until they're non-stale
        /// Default: 64 MB
        /// Minimum: 1 MB
        /// </summary>
        [DefaultValue(64)]
        [MinValue(1)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Indexing/NewIndexInMemoryMaxInMB")]
        [ConfigurationEntry("Raven/NewIndexInMemoryMaxMB")]
        public Size NewIndexInMemoryMaxSize { get; set; }

        /// <summary>
        /// Controls whatever RavenDB will create temporary indexes 
        /// for queries that cannot be directed to standard indexes
        /// </summary>
        [DefaultValue(true)]
        [ConfigurationEntry("Raven/Indexing/CreateAutoIndexesForAdHocQueriesIfNeeded")]
        [ConfigurationEntry("Raven/CreateAutoIndexesForAdHocQueriesIfNeeded")]
        public bool CreateAutoIndexesForAdHocQueriesIfNeeded { get; set; }

        /// <summary>
        /// When the database is shut down rudely, determine whatever to reset the index or to check it.
        /// Checking the index may take some time on large databases
        /// </summary>
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/ResetIndexOnUncleanShutdown")]
        [ConfigurationEntry("Raven/ResetIndexOnUncleanShutdown")]
        public bool ResetIndexOnUncleanShutdown { get; set; }

        /// <summary>
        /// Prevent index from being kept in memory. Default: false
        /// </summary>
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/DisableInMemory")]
        [ConfigurationEntry("Raven/DisableInMemoryIndexing")]
        public bool DisableInMemoryIndexing { get; set; }

        /// <summary>
        /// Maximum time interval for storing commit points for map indexes when new items were added.
        /// The commit points are used to restore index if unclean shutdown was detected.
        /// Default: 00:05:00 
        /// </summary>
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/MaxIndexCommitPointStoreIntervalInMin")]
        [ConfigurationEntry("Raven/MaxIndexCommitPointStoreTimeInterval")]
        public TimeSetting MaxIndexCommitPointStoreInterval { get; set; }

        /// <summary>
        /// Maximum number of kept commit points to restore map index after unclean shutdown
        /// Default: 5
        /// </summary>
        [DefaultValue(5)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfStoredCommitPoints")]
        [ConfigurationEntry("Raven/MaxNumberOfStoredCommitPoints")]
        public int MaxNumberOfStoredCommitPoints { get; set; }

        /// <summary>
        /// Minimum interval between between successive indexing that will allow to store a  commit point
        /// Default: 00:01:00
        /// </summary>
        [DefaultValue(1)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/MinIndexingIntervalToStoreCommitPointInMin")]
        [ConfigurationEntry("Raven/MinIndexingTimeIntervalToStoreCommitPoint")]
        public TimeSetting MinIndexingIntervalToStoreCommitPoint { get; set; }

        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeRunningIdleIndexesInMin")]
        [ConfigurationEntry("Raven/TimeToWaitBeforeRunningIdleIndexes")]
        public TimeSetting TimeToWaitBeforeRunningIdleIndexes { get; internal set; }

        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
        [ConfigurationEntry("Raven/TimeToWaitBeforeMarkingAutoIndexAsIdle")]
        public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; set; }

        [DefaultValue(72)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingIdleIndexAsAbandonedInHrs")]
        [ConfigurationEntry("Raven/TimeToWaitBeforeMarkingIdleIndexAsAbandoned")]
        public TimeSetting TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; set; }

        [DefaultValue(3)]
        [TimeUnit(TimeUnit.Hours)]
        [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeRunningAbandonedIndexesInHrs")]
        [ConfigurationEntry("Raven/TimeToWaitBeforeRunningAbandonedIndexes")]
        public TimeSetting TimeToWaitBeforeRunningAbandonedIndexes { get; set; }

        [DefaultValue(512)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfItemsToProcessInTestIndexes")]
        public int MaxNumberOfItemsToProcessInTestIndexes { get; set; }

        [DefaultValue(2048)]
        [ConfigurationEntry("Raven/Indexing/DisableIndexingFreeSpaceThreshold")]
        public int DisableIndexingFreeSpaceThreshold { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/DisableMapReduceInMemoryTracking")]
        public bool DisableMapReduceInMemoryTracking { get; set; }

        [DefaultValue(512)]
        [ConfigurationEntry("Raven/Indexing/MaxNumberOfStoredIndexingBatchInfoElements")]
        public int MaxNumberOfStoredIndexingBatchInfoElements { get; set; }

        [DefaultValue(true)]
        [ConfigurationEntry("Raven/Indexing/UseLuceneASTParser")]
        public bool UseLuceneASTParser
        {
            get { return useLuceneASTParser; }
            set
            {
                if (value == useLuceneASTParser)
                    return;
                QueryBuilder.UseLuceneASTParser = useLuceneASTParser = value;
            }
        }

        /// <summary>
        /// Indexes are flushed to a disk only if their in-memory size exceed the specified value. Default: 5MB
        /// </summary>
        [DefaultValue(5)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Indexing/FlushIndexToDiskSizeInMB")]
        public Size FlushIndexToDiskSize { get; set; }

        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Indexing/Disable")]
        [ConfigurationEntry("Raven/IndexingDisabled")]
        public bool Disabled { get; set; }
    }
}