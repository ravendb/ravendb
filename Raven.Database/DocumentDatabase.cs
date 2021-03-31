//-----------------------------------------------------------------------
// <copyright file="DocumentDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Lucene.Net.Support;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.MiniMetrics;
using Raven.Bundles.Replication.Triggers;
using Raven.Database.Actions;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Config.Retriever;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Impl.BackgroundTaskExecuter;
using Raven.Database.Impl.DTC;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Server.Connections;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Common;
using Raven.Database.Json;
using Raven.Database.Raft;
using Raven.Database.Server.WebApi;
using Voron;
using ThreadState = System.Threading.ThreadState;

namespace Raven.Database
{
    public class DocumentDatabase : IResourceStore, IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private static int buildVersion = -1;

        private static string productVersion;

        private readonly TaskScheduler backgroundTaskScheduler;

        private readonly ThreadLocal<DisableTriggerState> disableAllTriggers = new ThreadLocal<DisableTriggerState>(() => new DisableTriggerState{Disabled = false});

        private readonly object idleLocker = new object();

        private readonly InFlightTransactionalState inFlightTransactionalState;

        private readonly IndexingExecuter indexingExecuter;

        private readonly LastCollectionEtags lastCollectionEtags;

        private readonly LastMapCompletedDatesPerCollection lastMapCompletedDatesPerCollection;

        private readonly Prefetcher prefetcher;

        private readonly SequentialUuidGenerator uuidGenerator;

        private readonly List<IDisposable> toDispose = new List<IDisposable>();

        private readonly TransportState transportState;

        private readonly WorkContext workContext;

        private volatile bool indexingWorkersStoppedManually;

        private volatile bool disposed;

        private readonly DocumentDatabaseInitializer initializer;

        public readonly FixedSizeConcurrentQueue<AutoTunerDecisionDescription> AutoTuningTrace = new FixedSizeConcurrentQueue<AutoTunerDecisionDescription>(100);

        public class IndexFailDetails
        {
            public string IndexName;
            public string Reason;
            public Exception Ex;
        }

        public RavenThreadPool ThreadPool { get; set; }

        public DocumentDatabase(InMemoryRavenConfiguration configuration, DocumentDatabase systemDatabase, TransportState recievedTransportState = null, Action<object, Exception> onError = null)
        {
            TimerManager = new ResourceTimerManager();
            DocumentLock = new PutSerialLock(configuration);
            IdentityLock = new PutSerialLock(configuration);
            Name = configuration.DatabaseName;
            ResourceName = Name;
            Configuration = configuration;
            
            // initialize thread pool
            if (systemDatabase == null)
            {
                ThreadPool = new RavenThreadPool(configuration.MaxNumberOfParallelProcessingTasks * 2);
                ThreadPool.Start();
                toDispose.Add(ThreadPool);
                ThreadPool.ReportToAutoTuner = this.AutoTuningTrace.Enqueue;
                ThreadPool.ReportAlert = this.AddAlert;
            }
            else
            {
                ThreadPool = systemDatabase.ThreadPool;
            }

            transportState = recievedTransportState ?? new TransportState();
            ExtensionsState = new AtomicDictionary<object>();

            using (LogManager.OpenMappedContext("database", Name ?? Constants.SystemDatabase))
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Start loading the following database: {0}", Name ?? Constants.SystemDatabase);

                initializer = new DocumentDatabaseInitializer(this, configuration);
                initializer.ValidateLicense();

                initializer.ValidateStorage();

                initializer.InitializeEncryption();

                initializer.SubscribeToDomainUnloadOrProcessExit();
                initializer.SubscribeToDiskSpaceChanges();
                initializer.ExecuteAlterConfiguration();
                initializer.SatisfyImportsOnce();

                backgroundTaskScheduler = configuration.CustomTaskScheduler ?? TaskScheduler.Default;

                workContext = new WorkContext
                {
                    Database = this,
                    DatabaseName = Name,
                    IndexUpdateTriggers = IndexUpdateTriggers,
                    ReadTriggers = ReadTriggers,
                    TaskScheduler = backgroundTaskScheduler,
                    Configuration = configuration,
                    IndexReaderWarmers = IndexReaderWarmers
                };

                try
                {
                    uuidGenerator = new SequentialUuidGenerator();
                    initializer.InitializeTransactionalStorage(uuidGenerator, onError);
                    lastCollectionEtags = new LastCollectionEtags(WorkContext);
                }
                catch (Exception ex)
                {
                    Log.ErrorException("Could not initialize transactional storage, not creating database", ex);
                    try
                    {
                        if (TransactionalStorage != null)
                            TransactionalStorage.Dispose();
                        if (initializer != null)
                        {
                            initializer.UnsubscribeToDomainUnloadOrProcessExit();
                            initializer.Dispose();
                        }
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException("Could not dispose on initialized DocumentDatabase members", e);
                    }

                    throw;
                }

                try
                {
                    TransactionalStorage.Batch(actions => uuidGenerator.EtagBase = actions.General.GetNextIdentityValue("Raven/Etag"));
                    var reason = initializer.InitializeIndexDefinitionStorage();
                    Indexes = new IndexActions(this, uuidGenerator, Log);
                    Attachments = new AttachmentActions(this, uuidGenerator, Log);
                    Maintenance = new MaintenanceActions(this, uuidGenerator, Log);
                    Notifications = new NotificationActions(this, uuidGenerator, Log);
                    Subscriptions = new SubscriptionActions(this, Log);
                    Patches = new PatchActions(this, uuidGenerator, Log);
                    Queries = new QueryActions(this, uuidGenerator, Log);
                    Tasks = new TaskActions(this, uuidGenerator, Log);
                    Transformers = new TransformerActions(this, uuidGenerator, Log);
                    Documents = new DocumentActions(this, uuidGenerator, Log);

                    lastMapCompletedDatesPerCollection = new LastMapCompletedDatesPerCollection(this);

                    ConfigurationRetriever = new ConfigurationRetriever(systemDatabase ?? this, this);

                    inFlightTransactionalState = TransactionalStorage.InitializeInFlightTransactionalState(this,
                        (key, etag, document, metadata, transactionInformation) => Documents.Put(key, etag, document, metadata, transactionInformation),
                        (key, etag, transactionInformation) => Documents.Delete(key, etag, transactionInformation));

                    InitializeTriggersExceptIndexCodecs();
                    // Second stage initializing before index storage for determining the hash algotihm for encrypted databases that were upgraded from 2.5
                    SecondStageInitialization();

                    // Index codecs must be initialized before we try to read an index
                    InitializeIndexCodecTriggers();
                    initializer.InitializeIndexStorage();

                    CompleteWorkContextSetup();

                    prefetcher = new Prefetcher(workContext);

                    IndexReplacer = new IndexReplacer(this);
                    indexingExecuter = new IndexingExecuter(workContext, prefetcher, IndexReplacer);

                    EnsureAllIndexDefinitionsHaveIndexes();

                    RaiseIndexingWiringComplete();

                    Maintenance.DeleteRemovedIndexes(reason);

                    ExecuteStartupTasks();
                    lastCollectionEtags.InitializeBasedOnIndexingResults();
                    ReducingExecuter = new ReducingExecuter(workContext, IndexReplacer);

                    if (Log.IsDebugEnabled)
                        Log.Debug("Finish loading the following database: {0}", configuration.DatabaseName ?? Constants.SystemDatabase);
                }
                catch (Exception e)
                {
                    Log.ErrorException("Could not create database", e);
                    try
                    {
                        Dispose();
                    }
                    catch (Exception ex)
                    {
                        Log.FatalException("Failed to disposed when already getting an error during ctor", ex);
                    }
                    throw;
                }
            }
        }

        private void EnsureAllIndexDefinitionsHaveIndexes()
        {
            // this code is here to make sure that all index defs in the storage have
            // matching indexes.
            foreach (var index in IndexDefinitionStorage.IndexNames)
            {
                if (IndexStorage.HasIndex(index))
                    continue;
                var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
                // here we have an index definition without an index
                Indexes.DeleteIndex(index);
                Indexes.PutIndex(index, indexDefinition);
            }
        }

        public event EventHandler Disposing;

        public event EventHandler DisposingEnded;

        public event EventHandler StorageInaccessible;

        public event Action OnIndexingWiringComplete;

        public event Action<DocumentDatabase> OnBackupComplete;

        public Action<DiskSpaceNotification> OnDiskSpaceChanged = delegate { };

        public static int BuildVersion
        {
            get
            {
                if (buildVersion == -1)
                {
                    var customAttributes = typeof(DocumentDatabase).Assembly.GetCustomAttributes(false);
                    dynamic versionAtt = customAttributes.Single(x => x.GetType().Name == "RavenVersionAttribute");
                    buildVersion = int.Parse(versionAtt.Build);
                }
                return buildVersion;
            }
        }

        public static string ProductVersion
        {
            get
            {
                if (!string.IsNullOrEmpty(productVersion))
                {
                    return productVersion;
                }

                var customAttributes = typeof(DocumentDatabase).Assembly.GetCustomAttributes(false);
                dynamic versionAtt = customAttributes.Single(x => x.GetType().Name == "RavenVersionAttribute");

                productVersion = versionAtt.CommitHash;
                return productVersion;
            }
        }

        public long ApproximateTaskCount
        {
            get
            {
                long approximateTaskCount = 0;
                TransactionalStorage.Batch(actions => { approximateTaskCount = actions.Tasks.ApproximateTaskCount; });
                return approximateTaskCount;
            }
        }

        [ImportMany]
        [Obsolete("Use RavenFS instead.")]
        public OrderedPartCollection<AbstractAttachmentDeleteTrigger> AttachmentDeleteTriggers { get; set; }

        [ImportMany]
        [Obsolete("Use RavenFS instead.")]
        public OrderedPartCollection<AbstractAttachmentPutTrigger> AttachmentPutTriggers { get; set; }

        [ImportMany]
        [Obsolete("Use RavenFS instead.")]
        public OrderedPartCollection<AbstractAttachmentReadTrigger> AttachmentReadTriggers { get; set; }

        internal PutSerialLock DocumentLock { get; private set; }

        internal PutSerialLock IdentityLock { get; private set; }

        [Obsolete("Use RavenFS instead.")]
        public AttachmentActions Attachments { get; private set; }

        public TaskScheduler BackgroundTaskScheduler => backgroundTaskScheduler;

        public InMemoryRavenConfiguration Configuration { get; private set; }

        public ConfigurationRetriever ConfigurationRetriever { get; private set; }

        [ImportMany]
        public OrderedPartCollection<AbstractDeleteTrigger> DeleteTriggers { get; set; }

        /// <summary>
        ///     Whatever this database has been disposed
        /// </summary>
        public bool Disposed => disposed;

        [ImportMany]
        public OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }

        public DocumentActions Documents { get; private set; }

        [ImportMany]
        public OrderedPartCollection<AbstractDynamicCompilationExtension> Extensions { get; set; }

        /// <summary>
        ///     This is used to hold state associated with this instance by external extensions
        /// </summary>
        public AtomicDictionary<object> ExtensionsState { get; private set; }

        public bool HasTasks
        {
            get
            {
                bool hasTasks = false;
                TransactionalStorage.Batch(actions => { hasTasks = actions.Tasks.HasTasks; });
                return hasTasks;
            }
        }

        [CLSCompliant(false)]
        public InFlightTransactionalState InFlightTransactionalState => inFlightTransactionalState;

        [ImportMany]
        public OrderedPartCollection<AbstractIndexCodec> IndexCodecs { get; set; }

        public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexQueryTrigger> IndexQueryTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexReaderWarmer> IndexReaderWarmers { get; set; }

        public IndexStorage IndexStorage { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }

        public IndexActions Indexes { get; private set; }

        [CLSCompliant(false)]
        public IndexingExecuter IndexingExecuter => indexingExecuter;

        public LastCollectionEtags LastCollectionEtags => lastCollectionEtags;

        public LastMapCompletedDatesPerCollection LastMapCompletedDatesPerCollection => lastMapCompletedDatesPerCollection;

        public MaintenanceActions Maintenance { get; private set; }

        /// <summary>
        ///     The name of the database.
        ///     Defaults to null for the root database (or embedded database), or the name of the database if this db is a tenant
        ///     database
        /// </summary>
        public string Name { get; private set; }

        public string ResourceName { get; private set; }

        public NotificationActions Notifications { get; private set; }

        public SubscriptionActions Subscriptions { get; private set; }

        public PatchActions Patches { get; private set; }

        public Prefetcher Prefetcher => prefetcher;

        [ImportMany]
        public OrderedPartCollection<AbstractPutTrigger> PutTriggers { get; set; }

        public QueryActions Queries { get; private set; }

        [ImportMany]
        public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }

        [CLSCompliant(false)]
        public ReducingExecuter ReducingExecuter { get; private set; }

        public ResourceTimerManager TimerManager { get; private set; }

        public IndexReplacer IndexReplacer { get; private set; }

        public string ServerUrl
        {
            get
            {
                string serverUrl = Configuration.ServerUrl;
                if (string.IsNullOrEmpty(Name))
                {
                    return serverUrl;
                }

                if (serverUrl.EndsWith("/"))
                {
                    return serverUrl + "databases/" + Name;
                }

                return serverUrl + "/databases/" + Name;
            }
        }

        [ImportMany]
        public OrderedPartCollection<IStartupTask> StartupTasks { get; set; }

        public PluginsInfo PluginsInfo
        {
            get
            {
                var triggerInfos = PutTriggers.Select(x => new TriggerInfo
                {
                    Name = x.ToString(),
                    Type = "Put"
                })
                   .Concat(DeleteTriggers.Select(x => new TriggerInfo
                   {
                       Name = x.ToString(),
                       Type = "Delete"
                   }))
                   .Concat(ReadTriggers.Select(x => new TriggerInfo
                   {
                       Name = x.ToString(),
                       Type = "Read"
                   }))
                   .Concat(IndexUpdateTriggers.Select(x => new TriggerInfo
                   {
                       Name = x.ToString(),
                       Type = "Index Update"
                   })).ToList();

                var types = new[]
                {
                    typeof (IStartupTask),
                    typeof (AbstractReadTrigger),
                    typeof (AbstractDeleteTrigger),
                    typeof (AbstractPutTrigger),
                    typeof (AbstractDocumentCodec),
                    typeof (AbstractIndexCodec),
                    typeof (AbstractDynamicCompilationExtension),
                    typeof (AbstractIndexQueryTrigger),
                    typeof (AbstractIndexUpdateTrigger),
                    typeof (AbstractAnalyzerGenerator),
                    typeof (AbstractAttachmentDeleteTrigger),
                    typeof (AbstractAttachmentPutTrigger),
                    typeof (AbstractAttachmentReadTrigger),
                    typeof (AbstractBackgroundTask),
                    typeof (IAlterConfiguration)
                };

                var extensions = Configuration.ReportExtensions(types).ToList();

                var customBundles = FindPluginBundles(types);
                return new PluginsInfo
                {
                    Triggers = triggerInfos,
                    Extensions = extensions,
                    CustomBundles = customBundles
                };
            }
        }

        private List<string> FindPluginBundles(Type[] types)
        {
            var unfilteredCatalogs = InMemoryRavenConfiguration.GetUnfilteredCatalogs(Configuration.Catalog.Catalogs);

            AggregateCatalog unfilteredAggregate = null;

            var innerAggregate = unfilteredCatalogs as AggregateCatalog;
            if (innerAggregate != null)
            {
                unfilteredAggregate = new AggregateCatalog(innerAggregate.Catalogs.OfType<BuiltinFilteringCatalog>());
            }

            if (unfilteredAggregate == null || unfilteredAggregate.Catalogs.Count == 0)
                return new List<string>();

            var bundles = unfilteredAggregate.SelectMany(x => x.ExportDefinitions)
                .Where(x => x.Metadata.ContainsKey("Bundle"))
                .OrderBy(x => x.Metadata.ContainsKey("Order")
                    ? (int)x.Metadata["Order"]
                    : int.MaxValue)
                .Select(x => x.Metadata["Bundle"] as string)
                .Distinct()
                .ToList();

            return bundles;
        }

        public IndexingPerformanceStatistics[] IndexingPerformanceStatistics => (from pair in IndexDefinitionStorage.IndexDefinitions
                                                                                 let performance = IndexStorage.GetIndexingPerformance(pair.Key)
                                                                                 select new IndexingPerformanceStatistics
                                                                                 {
                                                                                     IndexId = pair.Key,
                                                                                     IndexName = pair.Value.Name,
                                                                                     Performance = performance
                                                                                 }).ToArray();

        public DatabaseStatistics Statistics
        {
            get
            {
                var result = new DatabaseStatistics
                {
                    CurrentNumberOfParallelTasks = workContext.CurrentNumberOfParallelTasks,
                    StorageEngine = TransactionalStorage.FriendlyName,
                    CurrentNumberOfItemsToIndexInSingleBatch = workContext.CurrentNumberOfItemsToIndexInSingleBatch,
                    CurrentNumberOfItemsToReduceInSingleBatch = workContext.CurrentNumberOfItemsToReduceInSingleBatch,
                    InMemoryIndexingQueueSizes = prefetcher.GetInMemoryIndexingQueueSizes(PrefetchingUser.Indexer),
                    Prefetches = workContext.FutureBatchStats.OrderBy(x => x.Timestamp).ToArray(),
                    CountOfIndexes = IndexStorage.Indexes.Length,
                    CountOfResultTransformers = IndexDefinitionStorage.ResultTransformersCount,
                    DatabaseTransactionVersionSizeInMB = ConvertBytesToMBs(workContext.TransactionalStorage.GetDatabaseTransactionVersionSizeInBytes()),
                    Errors = workContext.Errors,
                    DatabaseId = TransactionalStorage.Id,
                    SupportsDtc = TransactionalStorage.SupportsDtc,
                    Is64Bit = Environment.Is64BitProcess,
                    IsMemoryStatisticThreadRuning = MemoryStatistics.LowMemoryWatcherThreadState == ThreadState.Background
                };

                TransactionalStorage.Batch(actions =>
                {
                    result.LastDocEtag = actions.Staleness.GetMostRecentDocumentEtag();
                    result.LastAttachmentEtag = actions.Staleness.GetMostRecentAttachmentEtag();

                    result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
                    result.CountOfDocuments = actions.Documents.GetDocumentsCount();
                    result.CountOfAttachments = actions.Attachments.GetAttachmentsCount();

                    result.StaleIndexes = IndexStorage.Indexes.Where(indexId => IndexStorage.IsIndexStale(indexId, LastCollectionEtags))
                    .Select(indexId =>
                    {
                        Index index = IndexStorage.GetIndexInstance(indexId);
                        return index == null ? null : index.PublicName;
                    }).ToArray();

                    result.Indexes = GetIndexesStats(actions, result.LastDocEtag);

                    result.CountOfIndexesExcludingDisabledAndAbandoned = result.Indexes.Count(idx => !idx.Priority.HasFlag(IndexingPriority.Disabled) && !idx.Priority.HasFlag(IndexingPriority.Abandoned));
                    result.CountOfStaleIndexesExcludingDisabledAndAbandoned = result.Indexes.Count(idx =>
                        result.StaleIndexes.Contains(idx.Name)
                        && !idx.Priority.HasFlag(IndexingPriority.Disabled)
                        && !idx.Priority.HasFlag(IndexingPriority.Abandoned));
                });

                GetMoreIndexesStats(result.Indexes);

                return result;
            }
        }

        public ReducedDatabaseStatistics ReducedStatistics
        {
            get
            {
                var result = new ReducedDatabaseStatistics
                {
                    DatabaseId = TransactionalStorage.Id,
                    CountOfErrors = WorkContext.Errors.Length,
                    CountOfIndexes = IndexStorage.Indexes.Length,
                    CountOfStaleIndexes = IndexStorage.Indexes.Count(indexId => IndexStorage.IsIndexStale(indexId, LastCollectionEtags)),
                    CountOfAlerts = GetNumberOfAlerts()
                };

                TransactionalStorage.Batch(actions =>
                {
                    result.CountOfDocuments = actions.Documents.GetDocumentsCount();

                    var lastDocEtag = actions.Staleness.GetMostRecentDocumentEtag();
                    var indexes = GetIndexesStats(actions, lastDocEtag);

                    result.CountOfIndexesExcludingDisabledAndAbandoned = indexes.Count(idx => !idx.Priority.HasFlag(IndexingPriority.Disabled) && !idx.Priority.HasFlag(IndexingPriority.Abandoned));
                    result.CountOfStaleIndexesExcludingDisabledAndAbandoned = IndexStorage.Indexes
                        .Where(indexId => IndexStorage.IsIndexStale(indexId, lastCollectionEtags))
                        .Count(indexId =>
                        {
                            var index = IndexStorage.GetIndexInstance(indexId);
                            return index != null;
                        });

                    result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
                    result.CountOfAttachments = actions.Attachments.GetAttachmentsCount();
                });

                return result;
            }
        }

        public IndexStats[] IndexesStatistics
        {
            get
            {
                IndexStats[] indexes = null;

                TransactionalStorage.Batch(actions =>
                {
                    var lastDocEtag = actions.Staleness.GetMostRecentDocumentEtag();
                    indexes = GetIndexesStats(actions, lastDocEtag);
                });

                GetMoreIndexesStats(indexes);

                return indexes;
            }
        }

        private void GetMoreIndexesStats(IndexStats[] indexes)
        {
            if (indexes == null)
                return;

            foreach (IndexStats index in indexes)
            {
                try
                {
                    IndexDefinition indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index.Id);
                    index.LastQueryTimestamp = IndexStorage.GetLastQueryTime(index.Id);
                    index.IsTestIndex = indexDefinition.IsTestIndex;
                    index.IsOnRam = IndexStorage.IndexOnRam(index.Id);
                    index.LockMode = indexDefinition.LockMode;
                    index.IsMapReduce = indexDefinition.IsMapReduce;

                    index.ForEntityName = IndexDefinitionStorage.GetViewGenerator(index.Id).ForEntityNames.ToArray();
                    IndexSearcher searcher;
                    using (IndexStorage.GetCurrentIndexSearcher(index.Id, out searcher))
                        index.DocsCount = searcher.IndexReader.NumDocs();
                }
                catch (Exception)
                {
                    // might happen if the index was deleted mid operation
                    // we don't really care for that, so we ignore this
                }
            }
        }

        private IndexStats[] GetIndexesStats(IStorageActionsAccessor actions, Etag lastDocEtag)
        {
            return actions.Indexing.GetIndexesStats()
                .Where(x => x != null).Select(x =>
                {
                    Index indexInstance = IndexStorage.GetIndexInstance(x.Id);
                    if (indexInstance == null)
                        return null;
                    x.Name = indexInstance.PublicName;
                    x.SetLastDocumentEtag(lastDocEtag);
                    return x;
                })
                .Where(x => x != null)
                .ToArray();
        }

        private int GetNumberOfAlerts()
        {
            var alertsDoc = Documents.Get(Constants.RavenAlerts, null);
            if (alertsDoc == null)
                return 0;

            try
            {
                var alertsDocument = alertsDoc.DataAsJson.JsonDeserialization<AlertsDocument>();
                return alertsDocument.Alerts.Count;
            }
            catch (Exception)
            {
                return 0;
            }
        }

        public class ReducedDatabaseStatistics
        {
            public Guid DatabaseId { get; set; }

            public long CountOfDocuments { get; set; }

            public int CountOfIndexesExcludingDisabledAndAbandoned { get; set; }

            public int CountOfStaleIndexesExcludingDisabledAndAbandoned { get; set; }

            public int CountOfErrors { get; set; }

            public int CountOfIndexes { get; set; }

            public int CountOfStaleIndexes { get; set; }

            public int CountOfAlerts { get; set; }

            public long ApproximateTaskCount { get; set; }

            public long CountOfAttachments { get; set; }
        }

        public Dictionary<string, RemainingReductionPerLevel> GetRemainingScheduledReductions()
        {
            Dictionary<string, RemainingReductionPerLevel> res = new Dictionary<string, RemainingReductionPerLevel>();
            TransactionalStorage.Batch(accessor =>
            {
                var remaining = accessor.MapReduce.GetRemainingScheduledReductionPerIndex();
                foreach (var keyValue in remaining)
                {
                    var index = IndexStorage.GetIndexInstance(keyValue.Key);
                    if (index == null) continue;
                    res[index.PublicName] = keyValue.Value;
                }
            });
            return res;
        }

        public TaskActions Tasks { get; private set; }

        [CLSCompliant(false)]
        public ITransactionalStorage TransactionalStorage { get; private set; }

        [CLSCompliant(false)]
        public TransformerActions Transformers { get; private set; }

        public TransportState TransportState => transportState;

        public WorkContext WorkContext => workContext;
        public RequestManager RequestManager { get; set; }
        public Reference<ClusterManager> ClusterManager { get; set; }

        public BatchResult[] Batch(IList<ICommandData> commands, CancellationToken token)
        {
            using (DocumentLock.Lock())
            {
                bool shouldRetryIfGotConcurrencyError = commands.All(x => ((x is PatchCommandData || IsScriptedPatchCommandDataWithoutEtagProperty(x)) && (x.Etag == null)));
                if (shouldRetryIfGotConcurrencyError)
                {
                    Stopwatch sp = Stopwatch.StartNew();
                    BatchResult[] result = BatchWithRetriesOnConcurrencyErrorsAndNoTransactionMerging(commands, token);
                    if (Log.IsDebugEnabled)
                        Log.Debug("Successfully executed {0} patch commands in {1}", commands.Count, sp.Elapsed);
                    return result;
                }

                BatchResult[] results = null;

                Stopwatch sp2 = null;
                if (Log.IsDebugEnabled)
                {
                    sp2 = Stopwatch.StartNew();
                }

                TransactionalStorage.Batch(
                    actions => { results = ProcessBatch(commands, token); });

                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Executed {commands.Count:#,#;;0} commands, " +
                              $"took: {sp2?.ElapsedMilliseconds:#,#;;0}ms");
                }

                return results;
            }
        }

        public void PrepareTransaction(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null)
        {
            using (DocumentLock.Lock())
            {
                try
                {
                    inFlightTransactionalState.Prepare(txId, resourceManagerId, recoveryInformation);
                    if (Log.IsDebugEnabled)
                        Log.Debug("Prepare of tx {0} completed", txId);
                }
                catch (Exception e)
                {
                    if (TransactionalStorage.HandleException(e))
                        return;
                    throw;
                }
            }
        }

        public void Commit(string txId)
        {
            if (TransactionalStorage.SupportsDtc == false)
                throw new InvalidOperationException("DTC is not supported by " + TransactionalStorage.FriendlyName + " storage.");

            try
            {
                using (DocumentLock.Lock())
                {
                    try
                    {
                        inFlightTransactionalState.Commit(txId);
                        if (Log.IsDebugEnabled)
                            Log.Debug("Commit of tx {0} completed", txId);
                        workContext.ShouldNotifyAboutWork(() => "DTC transaction commited");
                    }
                    finally
                    {
                        inFlightTransactionalState.Rollback(txId); // this is where we actually remove the tx
                    }
                }
            }
            catch (Exception e)
            {
                if (TransactionalStorage.HandleException(e))
                    return;

                throw;
            }
            finally
            {
                workContext.HandleWorkNotifications();
            }
        }

        public DatabaseMetrics CreateMetrics()
        {
            MetricsCountersManager metrics = WorkContext.MetricsCounters;
            return new DatabaseMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                DocsWritesPerSecond = Math.Round(metrics.DocsPerSecond.CurrentValue, 3),
                IndexedPerSecond = Math.Round(metrics.IndexedPerSecond.CurrentValue, 3),
                ReducedPerSecond = Math.Round(metrics.ReducedPerSecond.CurrentValue, 3),
                RequestsDuration = metrics.RequestDurationMetric.CreateHistogramData(),
                RequestDurationLastMinute = metrics.RequestDurationLastMinute.GetData(),
                Requests = metrics.ConcurrentRequests.CreateMeterData(),
                JsonDeserializationsPerSecond = JsonExtensions.JsonStreamDeserializationsPerSecond == null ? (MeterValue?)null : JsonExtensions.JsonStreamDeserializationsPerSecond.GetValue(),
                JsonDeserializedBytesPerSecond = JsonExtensions.JsonStreamDeserializedBytesPerSecond == null ? (MeterValue?)null : JsonExtensions.JsonStreamDeserializedBytesPerSecond.GetValue(),
                JsonSerializationsPerSecond = JsonExtensions.JsonStreamSerializationsPerSecond == null ? (MeterValue?)null : JsonExtensions.JsonStreamSerializationsPerSecond.GetValue(),
                JsonSerializedBytesPerSecond = JsonExtensions.JsonStreamSerializedBytesPerSecond == null ? (MeterValue?)null : JsonExtensions.JsonStreamSerializedBytesPerSecond.GetValue(),
                Gauges = metrics.Gauges,
                StaleIndexMaps = metrics.StaleIndexMaps.CreateHistogramData(),
                StaleIndexReduces = metrics.StaleIndexReduces.CreateHistogramData(),
                ReplicationBatchSizeMeter = metrics.ReplicationBatchSizeMeter.ToMeterDataDictionary(),
                ReplicationBatchSizeHistogram = metrics.ReplicationBatchSizeHistogram.ToHistogramDataDictionary(),
                ReplicationDurationHistogram = metrics.ReplicationDurationHistogram.ToHistogramDataDictionary()
            };
        }



        /// <summary>
        ///     This API is provided solely for the use of bundles that might need to run
        ///     without any other bundle interfering. Specifically, the replication bundle
        ///     need to be able to run without interference from any other bundle.
        /// </summary>
        /// <returns></returns>
        public IDisposable DisableAllTriggersForCurrentThread(HashSet<Type> except = null)
        {
            if (disposed)
                return new DisposableAction(() => { });

            var old = disableAllTriggers.Value;
            disableAllTriggers.Value = new DisableTriggerState{Disabled = true, Except = except };
            return new DisposableAction(() =>
            {
                if (disposed)
                    return;

                try
                {
                    disableAllTriggers.Value = old;
                }
                catch (ObjectDisposedException)
                {
                }
            });
        }

        public void Dispose()
        {
            if (disposed)
                return;

            if (Log.IsDebugEnabled)
                Log.Debug("Start shutdown the following database: {0}", Name ?? Constants.SystemDatabase);

            var metrics = WorkContext.MetricsCounters;

            // give it 3 seconds to complete requests
            for (int i = 0; i < 30 && Interlocked.Read(ref metrics.ConcurrentRequestsCount) > 0; i++)
            {
                Thread.Sleep(100);
            }

            if (EnvironmentUtils.RunningOnPosix)
                MemoryStatistics.StopPosixLowMemThread();

            EventHandler onDisposing = Disposing;

            if (onDisposing != null)
            {
                try
                {
                    onDisposing(this, EventArgs.Empty);
                }
                catch (Exception e)
                {
                    Log.WarnException("Error when notifying about db disposal, ignoring error and continuing with disposal", e);
                }
            }

            var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose of DatabaseDocument");

            exceptionAggregator.Execute(() =>
            {
                if (workContext != null)
                    workContext.StopWorkRude();
            });

            exceptionAggregator.Execute(() =>
            {
                if (prefetcher != null)
                    prefetcher.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                initializer.UnsubscribeToDomainUnloadOrProcessExit();
                disposed = true;
            });

            if (initializer != null)
            {
                exceptionAggregator.Execute(initializer.Dispose);
            }

            exceptionAggregator.Execute(() =>
            {
                if (ExtensionsState == null)
                    return;

                foreach (IDisposable value in ExtensionsState.Values.OfType<IDisposable>())
                    exceptionAggregator.Execute(value.Dispose);
            });

            exceptionAggregator.Execute(() =>
            {
                if (toDispose == null)
                    return;

                foreach (IDisposable shouldDispose in toDispose)
                    exceptionAggregator.Execute(shouldDispose.Dispose);
            });

            exceptionAggregator.Execute(() =>
            {
                if (Tasks != null)
                    Tasks.Dispose(exceptionAggregator);
            });

            exceptionAggregator.Execute(() =>
            {
                var disposable = backgroundTaskScheduler as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            });


            if (IndexStorage != null)
                exceptionAggregator.Execute(IndexStorage.Dispose);

            if (TransactionalStorage != null)
                exceptionAggregator.Execute(() => TransactionalStorage.Dispose());

            if (Configuration != null)
                exceptionAggregator.Execute(Configuration.Dispose);

            exceptionAggregator.Execute(disableAllTriggers.Dispose);

            if (workContext != null)
                exceptionAggregator.Execute(workContext.Dispose);

            if (TimerManager != null)
                exceptionAggregator.Execute(TimerManager.Dispose);

            if (AttachmentDeleteTriggers != null && AttachmentDeleteTriggers.Count > 0)
                exceptionAggregator.Execute(() => AttachmentDeleteTriggers.Apply(x => x.Dispose()));

            if (DeleteTriggers != null && DeleteTriggers.Count > 0)
                exceptionAggregator.Execute(() => DeleteTriggers.Apply(x => x.Dispose()));

            if (inFlightTransactionalState != null)
                exceptionAggregator.Execute(inFlightTransactionalState.Dispose);

            if (ConfigurationRetriever != null)
                exceptionAggregator.Execute(ConfigurationRetriever.Dispose);

            try
            {
                exceptionAggregator.ThrowIfNeeded();
            }
            finally
            {
                var onDisposingEnded = DisposingEnded;
                if (onDisposingEnded != null)
                {
                    try
                    {
                        onDisposingEnded(this, EventArgs.Empty);
                    }
                    catch (Exception e)
                    {
                        Log.WarnException("Error when notifying about db disposal ending, ignoring error and continuing with disposal", e);
                    }
                }
            }

            if (Log.IsDebugEnabled)
                Log.Debug("Finished shutdown the following database: {0}", Name ?? Constants.SystemDatabase);
        }

        /// <summary>
        ///     Get the total index storage size taken by the indexes on the disk.
        ///     This explicitly does NOT include in memory indexes.
        /// </summary>
        /// <remarks>
        ///     This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public long GetIndexStorageSizeOnDisk()
        {
            if (Configuration.RunInMemory)
                return 0;

            string[] indexes = Directory.GetFiles(Configuration.IndexStoragePath, "*.*", SearchOption.AllDirectories);
            long totalIndexSize = indexes.Sum(file =>
            {
                try
                {
                    return new FileInfo(file).Length;
                }
                catch (UnauthorizedAccessException)
                {
                    return 0;
                }
                catch (FileNotFoundException)
                {
                    return 0;
                }
            });

            return totalIndexSize;
        }

        /// <summary>
        ///     Get the total size taken by the database on the disk.
        ///     This explicitly does NOT include in memory indexes or in memory database.
        ///     It does include any reserved space on the file system, which may significantly increase
        ///     the database size.
        /// </summary>
        /// <remarks>
        ///     This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public long GetTotalSizeOnDisk()
        {
            if (Configuration.RunInMemory)
                return 0;

            return GetIndexStorageSizeOnDisk() + GetTransactionalStorageSizeOnDisk().AllocatedSizeInBytes;
        }

        /// <summary>
        ///     Get the total size taken by the database on the disk.
        ///     This explicitly does NOT include in memory database.
        ///     It does include any reserved space on the file system, which may significantly increase
        ///     the database size.
        /// </summary>
        /// <remarks>
        ///     This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public DatabaseSizeInformation GetTransactionalStorageSizeOnDisk()
        {
            return Configuration.RunInMemory ? DatabaseSizeInformation.Empty : TransactionalStorage.GetDatabaseSize();
        }

        public bool HasTransaction(string txId)
        {
            return inFlightTransactionalState.HasTransaction(txId);
        }

        public void Rollback(string txId)
        {
            inFlightTransactionalState.Rollback(txId);
        }

        public void RunIdleOperations()
        {
            bool tryEnter = Monitor.TryEnter(idleLocker);
            try
            {
                if (tryEnter == false)
                    return;
                WorkContext.LastIdleTime = SystemTime.UtcNow;
                TransportState.OnIdle();
                IndexStorage.RunIdleOperations();
                IndexReplacer.ReplaceIndexes(IndexDefinitionStorage.Indexes);
                Tasks.ClearCompletedPendingTasks();
            }
            finally
            {
                if (tryEnter)
                {
                    Monitor.Exit(idleLocker);
                }
            }
        }

        public void SpinBackgroundWorkers(bool manualStart = false)
        {
            if (manualStart == false && indexingWorkersStoppedManually)
                return;

            if (IsIndexingDisabled())
                return;

            indexingWorkersStoppedManually = false;

            workContext.StartWork();

            SpinMappingWorker();

            SpinReduceWorker();

            RaiseIndexingWiringComplete();
        }

        public void StopBackgroundWorkers()
        {
            workContext.StopWork();
        }

        public void StopIndexingWorkers(bool manualStop)
        {
            if (manualStop == false && indexingWorkersStoppedManually)
                return;

            workContext.StopIndexing();
            indexingWorkersStoppedManually = manualStop;
        }

        public void ForceLicenseUpdate()
        {
            initializer.validateLicense.ForceExecute(Configuration);
        }

        protected void RaiseIndexingWiringComplete()
        {
            Action indexingWiringComplete = OnIndexingWiringComplete;
            OnIndexingWiringComplete = null; // we can only init once, release all actions
            if (indexingWiringComplete != null)
                indexingWiringComplete();
        }

        private BatchResult[] BatchWithRetriesOnConcurrencyErrorsAndNoTransactionMerging(IList<ICommandData> commands, CancellationToken token)
        {
            int retries = 128;
            Random rand = null;
            while (true)
            {
                token.ThrowIfCancellationRequested();

                try
                {
                    BatchResult[] results = null;
                    TransactionalStorage.Batch(_ => results = ProcessBatch(commands, token));
                    return results;
                }
                catch (ConcurrencyException)
                {
                    if (retries-- >= 0)
                    {
                        if (rand == null)
                            rand = new Random();

                        Thread.Sleep(rand.Next(5, Math.Max(retries * 2, 10)));
                        continue;
                    }

                    throw;
                }
                catch (OptimisticConcurrencyViolationException e)
                {
                    throw new ConcurrencyException(e.Message);
                }
            }
        }

        private void CompleteWorkContextSetup()
        {
            workContext.RaiseIndexChangeNotification = Notifications.RaiseNotifications;
            workContext.IndexStorage = IndexStorage;
            workContext.TransactionalStorage = TransactionalStorage;
            workContext.IndexDefinitionStorage = IndexDefinitionStorage;
            workContext.RecoverIndexingErrors();
        }

        private static decimal ConvertBytesToMBs(long bytes)
        {
            return Math.Round(bytes / 1024.0m / 1024.0m, 2);
        }


        private void ExecuteStartupTasks()
        {
            using (LogContext.WithResource(Name))
            {
                foreach (var task in StartupTasks)
                {
                    var disposable = task.Value as IDisposable;
                    if (disposable != null)
                    {
                        toDispose.Add(disposable);
                    }
                    try
                    {
                        task.Value.Execute(this);
                    }
                    //We catch any exception so not to cause the server to crash
                    catch (Exception e)
                    {
                        LogErrorAndAddAlertOnStartupTaskException(task.GetType().FullName, e);
                    }
                }
            }
        }

        internal void LogErrorAndAddAlertOnStartupTaskException(string taskTypeName, Exception e)
        {
            var msg = $"An error was thrown from executing a startup task of type, {taskTypeName}, preventing its functionality from running.";
            var exceptionMessage = $"Message:{e.Message}{Environment.NewLine}StackTrace:{e.StackTrace}";
            var uniqueKey = $"{Name} startup task {taskTypeName} fatal error";
            if (e.InnerException != null && !(e is AggregateException))
            {
                exceptionMessage += $"{Environment.NewLine}Inner exception Message:{e.InnerException.Message}{Environment.NewLine}StackTrace:{e.InnerException.StackTrace}";
            }
            else if (e is AggregateException)
            {
                var ae = e as AggregateException;
                foreach (var a in ae.InnerExceptions)
                {
                    exceptionMessage += $"{Environment.NewLine}Aggregate exception Message:{a.Message}{Environment.NewLine}StackTrace:{a.StackTrace}";
                }
            }
            Log.FatalException(msg, e);
            this.AddAlert(new Alert { AlertLevel = AlertLevel.Error, CreatedAt = DateTime.UtcNow, Message = msg + Environment.NewLine, Exception = exceptionMessage, Title = "Fatal error in startup task", UniqueKey = uniqueKey });
        }

        private void InitializeIndexCodecTriggers()
        {
            IndexCodecs.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
        }

        private void InitializeTriggersExceptIndexCodecs()
        {
            DocumentCodecs // .Init(disableAllTriggers) // Document codecs should always be activated (RavenDB-576)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            PutTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            DeleteTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            ReadTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            IndexQueryTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentPutTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentDeleteTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentReadTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            IndexUpdateTriggers.Init(disableAllTriggers).OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
        }

        private static bool IsScriptedPatchCommandDataWithoutEtagProperty(ICommandData commandData)
        {
            var scriptedPatchCommandData = commandData as ScriptedPatchCommandData;

            const string ScriptEtagKey = "'@etag':";
            const string EtagKey = "etag";

            return scriptedPatchCommandData != null && scriptedPatchCommandData.Patch.Script.Replace(" ", string.Empty).Contains(ScriptEtagKey) == false && scriptedPatchCommandData.Patch.Values.ContainsKey(EtagKey) == false;
        }

        private BatchResult[] ProcessBatch(IList<ICommandData> commands, CancellationToken token)
        {
            var results = new BatchResult[commands.Count];
            var participatingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var command in commands)
            {
                participatingIds.Add(command.Key);
            }

            for (int index = 0; index < commands.Count; index++)
            {
                token.ThrowIfCancellationRequested();

                results[index] = commands[index].ExecuteBatch(this, participatingIds);
            }

            return results;
        }

        private void SecondStageInitialization()
        {
            DocumentCodecs
                .OfType<IRequiresDocumentDatabaseInitialization>()
                .Concat(PutTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(DeleteTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(IndexCodecs.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(IndexQueryTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(AttachmentPutTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(AttachmentDeleteTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(AttachmentReadTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Concat(IndexUpdateTriggers.OfType<IRequiresDocumentDatabaseInitialization>())
                .Apply(initialization => initialization.SecondStageInit());
        }

        private class DocumentDatabaseInitializer
        {
            private readonly DocumentDatabase database;

            private readonly InMemoryRavenConfiguration configuration;

            internal ValidateLicense validateLicense;

            public DocumentDatabaseInitializer(DocumentDatabase database, InMemoryRavenConfiguration configuration)
            {
                this.database = database;
                this.configuration = configuration;
            }

            public void ValidateStorage()
            {
                var storageEngineTypeName = configuration.SelectDatabaseStorageEngineAndFetchTypeName();
                if (InMemoryRavenConfiguration.VoronTypeName == storageEngineTypeName
                    && configuration.Storage.Voron.AllowOn32Bits == false &&
                    Environment.Is64BitProcess == false)
                {
                    throw new Exception("Voron is prone to failure in 32-bits mode. Use " + Constants.Voron.AllowOn32Bits + " to force voron in 32-bit process.");
                }

                if (string.IsNullOrEmpty(configuration.DefaultStorageTypeName) == false &&
                    configuration.DefaultStorageTypeName.Equals(storageEngineTypeName, StringComparison.OrdinalIgnoreCase) == false)
                {
                    throw new Exception(string.Format("The database is configured to use '{0}' storage engine, but it points to '{1}' data", configuration.DefaultStorageTypeName, storageEngineTypeName));
                }

                if (configuration.RunInMemory == false && string.IsNullOrEmpty(configuration.DataDirectory) == false && Directory.Exists(configuration.DataDirectory))
                {
                    var resourceTypeFiles = Directory.EnumerateFiles(configuration.DataDirectory, Constants.ResourceMarkerPrefix + "*").Select(Path.GetFileName).ToArray();

                    if (resourceTypeFiles.Length == 0)
                        return;

                    if (resourceTypeFiles.Length > 1)
                    {
                        throw new Exception(string.Format("The database directory cannot contain more than one resource file marker, but it contains: {0}", string.Join(", ", resourceTypeFiles)));
                    }

                    var resourceType = resourceTypeFiles[0];

                    if (resourceType.Equals(Constants.Database.DbResourceMarker) == false)
                    {
                        throw new Exception(string.Format("The database data directory contains data of a different resource kind: {0}", resourceType.Substring(Constants.ResourceMarkerPrefix.Length)));
                    }
                }
            }

            public void ValidateLicense()
            {
                if (configuration.IsTenantDatabase)
                    return;

                validateLicense = new ValidateLicense();
                validateLicense.Execute(configuration);
            }

            public void Dispose()
            {
                if (validateLicense != null)
                    validateLicense.Dispose();
            }

            public void SubscribeToDomainUnloadOrProcessExit()
            {
                AppDomain.CurrentDomain.DomainUnload += DomainUnloadOrProcessExit;
                AppDomain.CurrentDomain.ProcessExit += DomainUnloadOrProcessExit;
            }

            public void UnsubscribeToDomainUnloadOrProcessExit()
            {
                AppDomain.CurrentDomain.DomainUnload -= DomainUnloadOrProcessExit;
                AppDomain.CurrentDomain.ProcessExit -= DomainUnloadOrProcessExit;
            }

            public void InitializeEncryption()
            {
                if (configuration.IsTenantDatabase)
                    return;

                string fipsAsString;
                bool fips;

                if (Commercial.ValidateLicense.CurrentLicense.Attributes.TryGetValue("fips", out fipsAsString) && bool.TryParse(fipsAsString, out fips))
                {
                    if (!fips && configuration.Encryption.UseFips)
                        throw new InvalidOperationException("Your license does not allow you to use FIPS compliant encryption on the server.");
                }

                Encryptor.Initialize(configuration.Encryption.UseFips);
                Cryptography.FIPSCompliant = configuration.Encryption.UseFips;
            }

            private void DomainUnloadOrProcessExit(object sender, EventArgs eventArgs)
            {
                Dispose();
            }

            public void ExecuteAlterConfiguration()
            {
                try
                {
                    foreach (IAlterConfiguration alterConfiguration in configuration.Container.GetExportedValues<IAlterConfiguration>())
                    {
                        alterConfiguration.AlterConfiguration(configuration);
                    }
                }
                catch (ReflectionTypeLoadException e)
                {
                    //throw more informative exception
                    if (e.LoaderExceptions != null && e.LoaderExceptions.Length > 0)
                        throw e.LoaderExceptions.First();

                    throw;
                }
            }

            public void SatisfyImportsOnce()
            {
                configuration.Container.SatisfyImportsOnce(database);
            }

            public void InitializeTransactionalStorage(IUuidGenerator uuidGenerator, Action<object, Exception> onErrorAction = null)
            {
                string storageEngineTypeName = configuration.SelectDatabaseStorageEngineAndFetchTypeName();
                database.TransactionalStorage = configuration.CreateTransactionalStorage(storageEngineTypeName, database.WorkContext.HandleWorkNotifications, () =>
                {
                    if (database.StorageInaccessible != null)
                        database.StorageInaccessible(database, EventArgs.Empty);

                }, database.WorkContext.NestedTransactionEnter, database.WorkContext.NestedTransactionExit);

                database.TransactionalStorage.Initialize(uuidGenerator, database.DocumentCodecs, storagePath =>
                {
                    if (configuration.RunInMemory)
                        return;

                    var resourceTypeFile = Path.Combine(storagePath, Constants.Database.DbResourceMarker);

                    if (File.Exists(resourceTypeFile) == false)
                        using (File.Create(resourceTypeFile)) { }
                },onErrorAction);
            }

            public Dictionary<int, IndexFailDetails> InitializeIndexDefinitionStorage()
            {
                database.IndexDefinitionStorage = new IndexDefinitionStorage(configuration, database.TransactionalStorage, configuration.DataDirectory, database.Extensions);
                return database.IndexDefinitionStorage.Initialize();
            }

            public void InitializeIndexStorage()
            {
                database.IndexStorage = new IndexStorage(database.IndexDefinitionStorage, configuration, database);
            }

            public void SubscribeToDiskSpaceChanges()
            {
                database.OnDiskSpaceChanged = notification =>
                {
                    if (notification.PathType != PathType.Index)
                        return;

                    if (configuration.Indexing.DisableIndexingFreeSpaceThreshold < 0)
                        return;

                    var thresholdInMb = configuration.Indexing.DisableIndexingFreeSpaceThreshold;
                    var warningThresholdInMb = Math.Max(thresholdInMb * 2, 1024);
                    var freeSpaceInMb = (int)(notification.FreeSpaceInBytes / 1024 / 1024);

                    if (freeSpaceInMb <= thresholdInMb)
                    {
                        database.StopIndexingWorkers(false);

                        var alertTitle = string.Format("Index disk '{0}' has {1}MB ({2}%) of free space and it has reached the {3}MB threshold. Indexing was disabled.", notification.Path, freeSpaceInMb, (int)(notification.FreeSpaceInPercentage * 100), thresholdInMb);
                        Log.Error(alertTitle);

                        database.AddAlert(new Alert
                        {
                            AlertLevel = AlertLevel.Error,
                            CreatedAt = SystemTime.UtcNow,
                            Title = alertTitle,
                            UniqueKey = "Free space (index)"
                        });
                    }
                    else
                    {
                        if (freeSpaceInMb <= warningThresholdInMb)
                        {
                            var alertTitle = string.Format("Index disk '{0}' has {1}MB ({2}%) of free space. Indexing will be disabled when it reaches {3}MB.", notification.Path, freeSpaceInMb, (int)(notification.FreeSpaceInPercentage * 100), thresholdInMb);
                            Log.Warn(alertTitle);

                            database.AddAlert(new Alert
                            {
                                AlertLevel = AlertLevel.Warning,
                                CreatedAt = SystemTime.UtcNow,
                                Title = alertTitle,
                                UniqueKey = "Free space warning (index)"
                            });
                        }

                        database.SpinBackgroundWorkers(false);
                    }
                };
            }
        }

        public void RaiseBackupComplete()
        {
            var onOnBackupComplete = OnBackupComplete;
            if (onOnBackupComplete != null) onOnBackupComplete(this);
        }

        public Task MappingTask { get; private set; }
        public Task ReducingTask { get; private set; }

        private void SpinMappingWorker()
        {
            if (IsIndexingDisabled())
                return;

            workContext.StartMapping();

            if (MappingTask != null)
                return;

            MappingTask = new Task(RunMapIndexes, TaskCreationOptions.LongRunning);
            MappingTask.Start();
        }

        private void RunMapIndexes()
        {
            try
            {
                indexingExecuter.Execute();
            }
            catch (Exception e)
            {
                if (disposed == false && workContext.RunIndexing)
                {
                    var errorMessage = $"Error from mapping task in database {Name}, task terminated, this is very bad";
                    Log.FatalException(errorMessage, e);
                    this.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = DateTime.UtcNow,
                        Title = errorMessage,
                        UniqueKey = errorMessage,
                        Message = e.ToString(),
                        Exception = e.Message
                    });
                }
            }
            finally
            {
                MappingTask = null;
            }
        }

        public void SpinReduceWorker()
        {
            if (IsIndexingDisabled())
                return;

            workContext.StartReducing();

            if (ReducingTask != null)
                return;

            ReducingTask = new Task(RunReduceIndexes, TaskCreationOptions.LongRunning);
            ReducingTask.Start();
        }

        private void RunReduceIndexes()
        {
            try
            {
                ReducingExecuter.Execute();
            }
            catch (Exception ex)
            {
                if (disposed == false && workContext.RunReducing)
                {
                    var errorMessage = $"Error from reducing task in database {Name}, task terminated, this is very bad";
                    Log.FatalException(errorMessage, ex);
                    this.AddAlert(new Alert
                    {
                        AlertLevel = AlertLevel.Error,
                        CreatedAt = DateTime.UtcNow,
                        Title = errorMessage,
                        UniqueKey = errorMessage,
                        Message = ex.ToString(),
                        Exception = ex.Message
                    });
                }
            }
            finally
            {
                ReducingTask = null;
            }
        }

        public void StopReduceWorkers()
        {
            workContext.StopReducing();
        }

        public bool IsIndexingDisabled()
        {
            var disableIndexing = Configuration.Settings[Constants.IndexingDisabled];
            if (disableIndexing != null)
            {
                bool disableIndexingStatus;
                var res = bool.TryParse(disableIndexing, out disableIndexingStatus);
                if (res && disableIndexingStatus)
                    return true;
            }

            return false;
        }
    }
}