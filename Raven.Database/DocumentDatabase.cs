//-----------------------------------------------------------------------
// <copyright file="DocumentDatabase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Lucene.Net.Support;
using NetTopologySuite.Utilities;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Client.Indexes;
using Raven.Database.Actions;
using Raven.Database.Commercial;
using Raven.Database.Impl.DTC;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Prefetching;
using Raven.Database.Queries;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Constants = Raven.Abstractions.Data.Constants;
using Raven.Json.Linq;
using BitConverter = System.BitConverter;
using Index = Raven.Database.Indexing.Index;
using Task = System.Threading.Tasks.Task;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Database
{
    public class DocumentDatabase : IDisposable
    {
        private readonly InMemoryRavenConfiguration configuration;

        [ImportMany]
        public OrderedPartCollection<IStartupTask> StartupTasks { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractAttachmentPutTrigger> AttachmentPutTriggers { get; set; }
        public InFlightTransactionalState InFlightTransactionalState
        {
            get { return inFlightTransactionalState; }
        }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexQueryTrigger> IndexQueryTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractAttachmentDeleteTrigger> AttachmentDeleteTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractAttachmentReadTrigger> AttachmentReadTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractPutTrigger> PutTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractDeleteTrigger> DeleteTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexUpdateTrigger> IndexUpdateTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractReadTrigger> ReadTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractDynamicCompilationExtension> Extensions { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexCodec> IndexCodecs { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractIndexReaderWarmer> IndexReaderWarmers { get; set; }

        private readonly List<IDisposable> toDispose = new List<IDisposable>();

        private long pendingTaskCounter;
        private readonly ConcurrentDictionary<long, PendingTaskAndState> pendingTasks = new ConcurrentDictionary<long, PendingTaskAndState>();

        private readonly InFlightTransactionalState inFlightTransactionalState;

        private class PendingTaskAndState
        {
            public Task Task;
            public object State;
        }

        /// <summary>
        /// The name of the database.
        /// Defaults to null for the root database (or embedded database), or the name of the database if this db is a tenant database
        /// </summary>
        public string Name { get; private set; }

        private readonly WorkContext workContext;
        private readonly IndexingExecuter indexingExecuter;
        public IndexingExecuter IndexingExecuter
        {
            get { return indexingExecuter; }
        }

        private ReducingExecuter reducingExecuter;
        public ReducingExecuter ReducingExecuter
        {
            get { return reducingExecuter; }
        }

        private readonly DatabaseEtagSynchronizer etagSynchronizer;
        public DatabaseEtagSynchronizer EtagSynchronizer
        {
            get { return etagSynchronizer; }
        }

        private readonly Prefetcher prefetcher;
        public Prefetcher Prefetcher
        {
            get { return prefetcher; }
        }

        /// <summary>
        /// Requires to avoid having serialize writes to the same attachments
        /// </summary>
        private readonly ConcurrentDictionary<string, object> putAttachmentSerialLock = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// This is used to hold state associated with this instance by external extensions
        /// </summary>
        public AtomicDictionary<object> ExtensionsState { get; private set; }

        public TaskScheduler BackgroundTaskScheduler { get { return backgroundTaskScheduler; } }

        private readonly ThreadLocal<bool> disableAllTriggers = new ThreadLocal<bool>(() => false);
        private Task indexingBackgroundTask;
        private Task reducingBackgroundTask;
        private readonly TaskScheduler backgroundTaskScheduler;
        private readonly object idleLocker = new object();

        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        public NotificationActions Notifications { get; private set; }

        public DocumentDatabase(InMemoryRavenConfiguration configuration, TransportState transportState = null)
        {
            this.configuration = configuration;
            this.transportState = transportState ?? new TransportState();
            InitializeEncryption(configuration);

            using (LogManager.OpenMappedContext("database", configuration.DatabaseName ?? Constants.SystemDatabase))
            {
                log.Debug("Start loading the following database: {0}", configuration.DatabaseName ?? Constants.SystemDatabase);

                if (configuration.IsTenantDatabase == false)
                {
                    validateLicense = new ValidateLicense();
                    validateLicense.Execute(configuration);
                }
                AppDomain.CurrentDomain.DomainUnload += DomainUnloadOrProcessExit;
                AppDomain.CurrentDomain.ProcessExit += DomainUnloadOrProcessExit;

                Name = configuration.DatabaseName;
                backgroundTaskScheduler = configuration.CustomTaskScheduler ?? TaskScheduler.Default;

                ExtensionsState = new AtomicDictionary<object>();
                Configuration = configuration;

                ExecuteAlterConfiguration();

                configuration.Container.SatisfyImportsOnce(this);

                workContext = new WorkContext
                {
                    Database = this,
                    DatabaseName = Name,
                    IndexUpdateTriggers = IndexUpdateTriggers,
                    ReadTriggers = ReadTriggers,
                    RaiseIndexChangeNotification = Notifications.RaiseNotifications,
                    TaskScheduler = backgroundTaskScheduler,
                    Configuration = configuration,
                    IndexReaderWarmers = IndexReaderWarmers
                };

                var storageEngineTypeName = configuration.SelectStorageEngineAndFetchTypeName();
                if (string.Equals(InMemoryRavenConfiguration.VoronTypeName, storageEngineTypeName, StringComparison.OrdinalIgnoreCase) == false)
                {
                    if (Directory.Exists(configuration.DataDirectory) &&
                        Directory.EnumerateFileSystemEntries(configuration.DataDirectory).Any())
                        throw new InvalidOperationException(
                            string.Format(
                                "We do not allow to run on a storage engine other then Voron, while we are in the early pre-release phase of RavenDB 3.0. You are currently running on {0}",
                                storageEngineTypeName));

                    Trace.WriteLine("Forcing database to run on Voron - pre release behavior only, mind " + Path.GetFileName(Path.GetDirectoryName(configuration.DataDirectory)));
                    storageEngineTypeName = InMemoryRavenConfiguration.VoronTypeName;

                }

                TransactionalStorage = configuration.CreateTransactionalStorage(storageEngineTypeName, workContext.HandleWorkNotifications);

                try
                {
                    sequentialUuidGenerator = new SequentialUuidGenerator();
                    TransactionalStorage.Initialize(sequentialUuidGenerator, DocumentCodecs);
                    lastCollectionEtags = new LastCollectionEtags(TransactionalStorage);
                }
                catch (Exception)
                {
                    TransactionalStorage.Dispose();
                    throw;
                }

                try
                {

                    inFlightTransactionalState = TransactionalStorage.GetInFlightTransactionalState(Put, Delete);

                    TransactionalStorage.Batch(actions =>
                        sequentialUuidGenerator.EtagBase = actions.General.GetNextIdentityValue("Raven/Etag"));

                    // Index codecs must be initialized before we try to read an index
                    InitializeIndexCodecTriggers();

                    IndexDefinitionStorage = new IndexDefinitionStorage(
                        configuration,
                        TransactionalStorage,
                        configuration.DataDirectory,
                        configuration.Container.GetExportedValues<AbstractViewGenerator>(),
                        Extensions);

                    IndexStorage = new IndexStorage(IndexDefinitionStorage, configuration, this);

                    CompleteWorkContextSetup();

                    etagSynchronizer = new DatabaseEtagSynchronizer(TransactionalStorage);
                    prefetcher = new Prefetcher(workContext);
                    indexingExecuter = new IndexingExecuter(workContext, etagSynchronizer, prefetcher);

                    RaiseIndexingWiringComplete();

                    InitializeTriggersExceptIndexCodecs();
                    SecondStageInitialization();
                    ExecuteStartupTasks();
                    lastCollectionEtags.Initialize();
                    log.Debug("Finish loading the following database: {0}", configuration.DatabaseName ?? Constants.SystemDatabase);
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }
        }

        private static void InitializeEncryption(InMemoryRavenConfiguration configuration)
        {
            string fipsAsString;
            bool fips;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("fips", out fipsAsString) && bool.TryParse(fipsAsString, out fips))
            {
                if (!fips && configuration.UseFips)
                    throw new InvalidOperationException(
                        "Your license does not allow you to use FIPS compliant encryption on the server.");
            }

            Encryptor.Initialize(configuration.UseFips);
            Cryptography.FIPSCompliant = configuration.UseFips;
        }

        private void SecondStageInitialization()
        {
            DocumentCodecs.OfType<IRequiresDocumentDatabaseInitialization>()
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

        private void CompleteWorkContextSetup()
        {
            workContext.IndexStorage = IndexStorage;
            workContext.TransactionalStorage = TransactionalStorage;
            workContext.IndexDefinitionStorage = IndexDefinitionStorage;
        }

        private void DomainUnloadOrProcessExit(object sender, EventArgs eventArgs)
        {
            Dispose();
        }

        private void InitializeTriggersExceptIndexCodecs()
        {
            DocumentCodecs
                //.Init(disableAllTriggers) // Document codecs should always be activated (RavenDB-576)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            PutTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            DeleteTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            ReadTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            IndexQueryTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentPutTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentDeleteTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            AttachmentReadTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));

            IndexUpdateTriggers
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
        }

        private void InitializeIndexCodecTriggers()
        {
            IndexCodecs
                .Init(disableAllTriggers)
                .OfType<IRequiresDocumentDatabaseInitialization>().Apply(initialization => initialization.Initialize(this));
        }

        private void ExecuteAlterConfiguration()
        {
            foreach (var alterConfiguration in Configuration.Container.GetExportedValues<IAlterConfiguration>())
            {
                alterConfiguration.AlterConfiguration(Configuration);
            }
        }

        private void ExecuteStartupTasks()
        {
            using (LogContext.WithDatabase(Name))
            {
                foreach (var task in StartupTasks)
                {
                    var disposable = task.Value as IDisposable;
                    if (disposable != null)
                        toDispose.Add(disposable);
                    task.Value.Execute(this);
                }
            }
        }

        public DatabaseStatistics Statistics
        {
            get
            {
                var result = new DatabaseStatistics
                {
                    CurrentNumberOfItemsToIndexInSingleBatch = workContext.CurrentNumberOfItemsToIndexInSingleBatch,
                    CurrentNumberOfItemsToReduceInSingleBatch = workContext.CurrentNumberOfItemsToReduceInSingleBatch,
                    ActualIndexingBatchSize = workContext.LastActualIndexingBatchSize.ToArray(),
                    InMemoryIndexingQueueSize = prefetcher.GetInMemoryIndexingQueueSize(PrefetchingUser.Indexer),
                    Prefetches = workContext.FutureBatchStats.OrderBy(x => x.Timestamp).ToArray(),
                    CountOfIndexes = IndexStorage.Indexes.Length,
                    DatabaseTransactionVersionSizeInMB = ConvertBytesToMBs(workContext.TransactionalStorage.GetDatabaseTransactionVersionSizeInBytes()),
                    Errors = workContext.Errors,
                    DatabaseId = TransactionalStorage.Id,
                    SupportsDtc = TransactionalStorage.SupportsDtc,
                    Triggers = PutTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Put" })
                        .Concat(DeleteTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Delete" }))
                        .Concat(ReadTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Read" }))
                        .Concat(IndexUpdateTriggers.Select(x => new DatabaseStatistics.TriggerInfo { Name = x.ToString(), Type = "Index Update" }))
                        .ToArray(),
                    Extensions = Configuration.ReportExtensions(
                        typeof(IStartupTask),
                        typeof(AbstractReadTrigger),
                        typeof(AbstractDeleteTrigger),
                        typeof(AbstractPutTrigger),
                        typeof(AbstractDocumentCodec),
                        typeof(AbstractIndexCodec),
                        typeof(AbstractDynamicCompilationExtension),
                        typeof(AbstractIndexQueryTrigger),
                        typeof(AbstractIndexUpdateTrigger),
                        typeof(AbstractAnalyzerGenerator),
                        typeof(AbstractAttachmentDeleteTrigger),
                        typeof(AbstractAttachmentPutTrigger),
                        typeof(AbstractAttachmentReadTrigger),
                        typeof(AbstractBackgroundTask),
                        typeof(IAlterConfiguration)
                        ),
                };
                TransactionalStorage.Batch(actions =>
                {
                    result.LastDocEtag = actions.Staleness.GetMostRecentDocumentEtag();
                    result.LastAttachmentEtag = actions.Staleness.GetMostRecentAttachmentEtag();

                    result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
                    result.CountOfDocuments = actions.Documents.GetDocumentsCount();
                    result.CountOfAttachments = actions.Attachments.GetAttachmentsCount();
                    result.StaleIndexes = IndexStorage.Indexes
                                                      .Where(indexId =>
                                                      {
                                                          var indexInstance = IndexStorage.GetIndexInstance(indexId);
                                                          return (indexInstance != null && indexInstance.IsMapIndexingInProgress) ||
                                                                                               actions.Staleness.IsIndexStale(indexId, null, null);
                                                      })
                                                      .Select(indexId =>
                                                      {
                                                          var index = IndexStorage.GetIndexInstance(indexId);
                                                          return index == null ? null : index.PublicName;
                                                      })
                                                      .ToArray();
                    result.Indexes = actions.Indexing.GetIndexesStats().Where(x => x != null)
                        .Select(x =>
                        {
                            var indexInstance = IndexStorage.GetIndexInstance(x.Id);
                            if (indexInstance != null)
                                x.PublicName = indexInstance.PublicName;
                            return x;
                        })
                        .ToArray();
                });

                if (result.Indexes != null)
                {
                    foreach (var index in result.Indexes)
                    {
                        try
                        {
                            var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index.Id);
                            index.LastQueryTimestamp = IndexStorage.GetLastQueryTime(index.Id);
                            index.Performance = IndexStorage.GetIndexingPerformance(index.Id);
                            index.IsOnRam = IndexStorage.IndexOnRam(index.Id);
                            if (indexDefinition != null)
                                index.LockMode = indexDefinition.LockMode;
                            index.ForEntityName = IndexDefinitionStorage.GetViewGenerator(index.Id).ForEntityNames.ToList();
                            IndexSearcher searcher;
                            using (IndexStorage.GetCurrentIndexSearcher(index.Id, out searcher))
                            {
                                index.DocsCount = searcher.IndexReader.NumDocs();
                            }

                        }
                        catch (Exception)
                        {
                            // might happen if the index was deleted mid operation
                            // we don't really care for that, so we ignore this
                        }
                    }
                }

                return result;
            }
        }


        private decimal ConvertBytesToMBs(long bytes)
        {
            return Math.Round(bytes / 1024.0m / 1024.0m, 2);
        }

        public InMemoryRavenConfiguration Configuration
        {
            get;
            private set;
        }

        public ITransactionalStorage TransactionalStorage { get; private set; }

        public IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

        public IndexStorage IndexStorage { get; private set; }

        public event EventHandler Disposing;

        public void Dispose()
        {
            if (disposed)
                return;

            log.Debug("Start shutdown the following database: {0}", Name ?? Constants.SystemDatabase);

            var onDisposing = Disposing;
            if (onDisposing != null)
            {
                try
                {
                    onDisposing(this, EventArgs.Empty);
                }
                catch (Exception e)
                {
                    log.WarnException("Error when notifying about db disposal, ignoring error and continuing with disposal", e);
                }
            }

            var exceptionAggregator = new ExceptionAggregator(log, "Could not properly dispose of DatabaseDocument");

            exceptionAggregator.Execute(() =>
            {
                if (lastCollectionEtags != null)
                    lastCollectionEtags.Flush();
            });

            exceptionAggregator.Execute(() =>
            {
                AppDomain.CurrentDomain.DomainUnload -= DomainUnloadOrProcessExit;
                AppDomain.CurrentDomain.ProcessExit -= DomainUnloadOrProcessExit;
                disposed = true;

                if (workContext != null)
                    workContext.StopWorkRude();
            });

            if (validateLicense != null)
                exceptionAggregator.Execute(validateLicense.Dispose);

            exceptionAggregator.Execute(() =>
            {
                if (ExtensionsState == null)
                    return;

                foreach (var value in ExtensionsState.Values.OfType<IDisposable>())
                {
                    exceptionAggregator.Execute(value.Dispose);
                }
            });

            exceptionAggregator.Execute(() =>
            {
                if (toDispose == null)
                    return;
                foreach (var shouldDispose in toDispose)
                {
                    exceptionAggregator.Execute(shouldDispose.Dispose);
                }
            });


            exceptionAggregator.Execute(() =>
            {
                foreach (var shouldDispose in pendingTasks)
                {
                    var pendingTaskAndState = shouldDispose.Value;
                    exceptionAggregator.Execute(() =>
                    {
                        try
                        {
#if DEBUG
                            pendingTaskAndState.Task.Wait(3000);
#else
							pendingTaskAndState.Task.Wait();
#endif
                        }
                        catch (Exception)
                        {
                            // we explictly don't care about this during shutdown
                        }
                    });
                }
                pendingTasks.Clear();
            });

            exceptionAggregator.Execute(() =>
            {
                if (indexingBackgroundTask != null)
                    indexingBackgroundTask.Wait();
            });
            exceptionAggregator.Execute(() =>
            {
                if (reducingBackgroundTask != null)
                    reducingBackgroundTask.Wait();
            });

            exceptionAggregator.Execute(() =>
            {
                var disposable = backgroundTaskScheduler as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            });

            if (TransactionalStorage != null)
                exceptionAggregator.Execute(TransactionalStorage.Dispose);
            if (IndexStorage != null)
                exceptionAggregator.Execute(IndexStorage.Dispose);

            if (Configuration != null)
                exceptionAggregator.Execute(Configuration.Dispose);

            exceptionAggregator.Execute(disableAllTriggers.Dispose);

            if (workContext != null)
                exceptionAggregator.Execute(workContext.Dispose);

            exceptionAggregator.ThrowIfNeeded();

            log.Debug("Finished shutdown the following database: {0}", Name ?? Constants.SystemDatabase);
        }

        public void StopBackgroundWorkers()
        {
            workContext.StopWork();
            if (indexingBackgroundTask != null)
                indexingBackgroundTask.Wait();
            if (reducingBackgroundTask != null)
                reducingBackgroundTask.Wait();

            backgroundWorkersSpun = false;
        }

        public void StopIndexingWorkers()
        {
            workContext.StopIndexing();
            try
            {
                indexingBackgroundTask.Wait();
            }
            catch (Exception e)
            {
                log.WarnException("Error while trying to stop background indexing", e);
            }
            try
            {
                reducingBackgroundTask.Wait();
            }
            catch (Exception e)
            {
                log.WarnException("Error while trying to stop background reducing", e);
            }

            backgroundWorkersSpun = false;
        }

        public WorkContext WorkContext
        {
            get { return workContext; }
        }

        private volatile bool backgroundWorkersSpun;

        public void SpinBackgroundWorkers()
        {
            if (backgroundWorkersSpun)
                throw new InvalidOperationException("The background workers has already been spun and cannot be spun again");

            backgroundWorkersSpun = true;

            workContext.StartWork();
            indexingBackgroundTask = Task.Factory.StartNew(
                indexingExecuter.Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);

            reducingExecuter = new ReducingExecuter(workContext);

            reducingBackgroundTask = Task.Factory.StartNew(
                reducingExecuter.Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
        }

        public void SpinIndexingWorkers()
        {
            if (backgroundWorkersSpun)
                throw new InvalidOperationException("The background workers has already been spun and cannot be spun again");

            backgroundWorkersSpun = true;

            workContext.StartIndexing();
            indexingBackgroundTask = Task.Factory.StartNew(
                indexingExecuter.Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);

            reducingExecuter = new ReducingExecuter(workContext);

            reducingBackgroundTask = Task.Factory.StartNew(
                reducingExecuter.Execute,
                CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
        }

        

        protected void RaiseIndexingWiringComplete()
        {
            var indexingWiringComplete = OnIndexingWiringComplete;
            OnIndexingWiringComplete = null; // we can only init once, release all actions
            if (indexingWiringComplete != null)
                indexingWiringComplete();
        }

        public event Action<DocumentDatabase, DocumentChangeNotification, RavenJObject> OnDocumentChange;

        public event Action OnIndexingWiringComplete;

        public void RunIdleOperations()
        {
            var tryEnter = Monitor.TryEnter(idleLocker);
            try
            {
                if (tryEnter == false)
                    return;
                TransportState.OnIdle();
                IndexStorage.RunIdleOperations();
                ClearCompletedPendingTasks();
                lastCollectionEtags.Flush();
            }
            finally
            {
                if (tryEnter)
                    Monitor.Exit(idleLocker);
            }
        }


        

        public bool HasTransaction(string txId)
        {
            return inFlightTransactionalState.HasTransaction(txId);
        }

        public void PrepareTransaction(string txId)
        {
            if (TransactionalStorage.SupportsDtc == false)
                throw new InvalidOperationException("DTC is not supported by " + TransactionalStorage.FriendlyName + " storage.");

            try
            {
                inFlightTransactionalState.Prepare(txId);
                log.Debug("Prepare of tx {0} completed", txId);
            }
            catch (Exception e)
            {
                if (TransactionalStorage.HandleException(e))
                    return;
                throw;
            }
        }

        public void Commit(string txId)
        {
            if (TransactionalStorage.SupportsDtc == false)
                throw new InvalidOperationException("DTC is not supported by " + TransactionalStorage.FriendlyName + " storage.");

            try
            {
                try
                {
                    inFlightTransactionalState.Commit(txId);
                    log.Debug("Commit of tx {0} completed", txId);
                    workContext.ShouldNotifyAboutWork(() => "DTC transaction commited");
                }
                finally
                {
                    inFlightTransactionalState.Rollback(txId); // this is where we actually remove the tx
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


        public void Rollback(string txId)
        {
            inFlightTransactionalState.Rollback(txId);
        }

        public BatchResult[] Batch(IList<ICommandData> commands)
        {
            using (TransactionalStorage.WriteLock())
            {				
                var shouldRetryIfGotConcurrencyError =
                commands.All(x => ((x is PatchCommandData || IsScriptedPatchCommandDataWithoutEtagProperty(x)) && (x.Etag == null)));
                if (shouldRetryIfGotConcurrencyError)
                {
                    var sp = Stopwatch.StartNew();
					var result = BatchWithRetriesOnConcurrencyErrorsAndNoTransactionMerging(commands);
                    log.Debug("Successfully executed {0} patch commands in {1}", commands.Count, sp.Elapsed);
                    return result;
                }

				BatchResult[] results = null;
                TransactionalStorage.Batch(actions =>
                {
                    results = ProcessBatch(commands);
                });

                return results;
            }
        }

	    private bool IsScriptedPatchCommandDataWithoutEtagProperty(ICommandData commandData)
	    {
		    var scriptedPatchCommandData = commandData as ScriptedPatchCommandData;

		    const string scriptEtagKey = "'@etag':";
		    const string etagKey = "etag";		    

		    return scriptedPatchCommandData != null &&
				   scriptedPatchCommandData.Patch.Script.Replace(" ",String.Empty).Contains(scriptEtagKey) == false &&
				   scriptedPatchCommandData.Patch.Values.ContainsKey(etagKey) == false;
	    }

        private BatchResult[] BatchWithRetriesOnConcurrencyErrorsAndNoTransactionMerging(IList<ICommandData> commands)
        {
            int retries = 128;
            Random rand = null;
            while (true)
            {
                try
                {
                    BatchResult[] results = null;
                    TransactionalStorage.Batch(_ => results = ProcessBatch(commands));
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
            }
        }

        private BatchResult[] ProcessBatch(IList<ICommandData> commands)
        {
            var results = new BatchResult[commands.Count];
            for (int index = 0; index < commands.Count; index++)
            {
                var command = commands[index];
                results[index] = command.ExecuteBatch(this);
            }
            return results;
        }

        public bool HasTasks
        {
            get
            {
                bool hasTasks = false;
                TransactionalStorage.Batch(actions =>
                {
                    hasTasks = actions.Tasks.HasTasks;
                });
                return hasTasks;
            }
        }

        public long ApproximateTaskCount
        {
            get
            {
                long approximateTaskCount = 0;
                TransactionalStorage.Batch(actions =>
                {
                    approximateTaskCount = actions.Tasks.ApproximateTaskCount;
                });
                return approximateTaskCount;
            }
        }

        

        public static void Restore(RavenConfiguration configuration, string backupLocation, string databaseLocation, Action<string> output, bool defrag)
        {
            var databaseDocumentPath = Path.Combine(backupLocation, "Database.Document");
            if (File.Exists(databaseDocumentPath) == false)
                throw new InvalidOperationException("Cannot restore when the Database.Document file is missing in the backup folder: " + backupLocation);

            var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
            var databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();

            string storage;
            if (databaseDocument.Settings.TryGetValue("Raven/StorageTypeName", out storage) == false)
            {
                storage = "esent";
            }

            if (!string.IsNullOrWhiteSpace(databaseLocation))
                configuration.DataDirectory = databaseLocation;

            using (var transactionalStorage = configuration.CreateTransactionalStorage(storage, () => { }))
            {
                transactionalStorage.Restore(backupLocation, databaseLocation, output, defrag);
            }
        }

        

        

        static string buildVersion;
        public static string BuildVersion
        {
            get
            {
                return buildVersion ??
                       (buildVersion = GetBuildVersion().ToString(CultureInfo.InvariantCulture));
            }
        }

        private static int GetBuildVersion()
        {
            var location = AssemblyHelper.GetAssemblyLocationFor<DocumentDatabase>();

            var fileVersionInfo = FileVersionInfo.GetVersionInfo(location);
            if (fileVersionInfo.FilePrivatePart != 0)
                return fileVersionInfo.FilePrivatePart;

            return fileVersionInfo.FileBuildPart;
        }

        private volatile bool disposed;
        private readonly ValidateLicense validateLicense;
        public string ServerUrl
        {
            get
            {
                var serverUrl = Configuration.ServerUrl;
                if (string.IsNullOrEmpty(Name))
                    return serverUrl;
                if (serverUrl.EndsWith("/"))
                    return serverUrl + "databases/" + Name;
                return serverUrl + "/databases/" + Name;
            }
        }

        static string productVersion;
        private readonly SequentialUuidGenerator sequentialUuidGenerator;
        private readonly TransportState transportState;
        private readonly LastCollectionEtags lastCollectionEtags;

        public static string ProductVersion
        {
            get
            {
                if (!string.IsNullOrEmpty(productVersion))
                    return productVersion;

                productVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<DocumentDatabase>()).ProductVersion;
                return productVersion;
            }
        }

        public string[] GetIndexFields(string index)
        {
            var abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
            if (abstractViewGenerator == null)
                return new string[0];
            return abstractViewGenerator.Fields;
        }

        /// <summary>
        /// This API is provided solely for the use of bundles that might need to run
        /// without any other bundle interfering. Specifically, the replication bundle
        /// need to be able to run without interference from any other bundle.
        /// </summary>
        /// <returns></returns>
        public IDisposable DisableAllTriggersForCurrentThread()
        {
            if (disposed)
                return new DisposableAction(() => { });
            var old = disableAllTriggers.Value;
            disableAllTriggers.Value = true;
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

        /// <summary>
        /// Whatever this database has been disposed
        /// </summary>
        public bool Disposed
        {
            get { return disposed; }
        }

        public TransportState TransportState
        {
            get
            {
                return transportState;
            }
        }
        public LastCollectionEtags LastCollectionEtags { get { return lastCollectionEtags; } }

        /// <summary>
        /// Get the total index storage size taken by the indexes on the disk.
        /// This explicitly does NOT include in memory indexes.
        /// </summary>
        /// <remarks>
        /// This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public long GetIndexStorageSizeOnDisk()
        {
            if (Configuration.RunInMemory)
                return 0;
            var indexes = Directory.GetFiles(Configuration.IndexStoragePath, "*.*", SearchOption.AllDirectories);
            var totalIndexSize = indexes.Sum(file =>
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
        /// Get the total size taken by the database on the disk.
        /// This explicitly does NOT include in memory database.
        /// It does include any reserved space on the file system, which may significantly increase
        /// the database size.
        /// </summary>
        /// <remarks>
        /// This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public DatabaseSizeInformation GetTransactionalStorageSizeOnDisk()
        {
            return Configuration.RunInMemory ? DatabaseSizeInformation.Empty : TransactionalStorage.GetDatabaseSize();
        }

        /// <summary>
        /// Get the total size taken by the database on the disk.
        /// This explicitly does NOT include in memory indexes or in memory database.
        /// It does include any reserved space on the file system, which may significantly increase
        /// the database size.
        /// </summary>
        /// <remarks>
        /// This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        public long GetTotalSizeOnDisk()
        {
            if (Configuration.RunInMemory)
                return 0;
            return GetIndexStorageSizeOnDisk() + GetTransactionalStorageSizeOnDisk().AllocatedSizeInBytes;
        }



        

        

       

   

        public DatabaseMetrics CreateMetrics()
        {
            var metrics = WorkContext.MetricsCounters;

            var percentiles = metrics.RequestDuationMetric.Percentiles(0.5, 0.75, 0.95, 0.99, 0.999, 0.9999);

            return new DatabaseMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                DocsWritesPerSecond = Math.Round(metrics.DocsPerSecond.CurrentValue, 3),
                IndexedPerSecond = Math.Round(metrics.IndexedPerSecond.CurrentValue, 3),
                ReducedPerSecond = Math.Round(metrics.ReducedPerSecond.CurrentValue, 3),

                RequestsDuration = new HistogramData
                {
                    Counter = metrics.RequestDuationMetric.Count,
                    Max = metrics.RequestDuationMetric.Max,
                    Mean = metrics.RequestDuationMetric.Mean,
                    Min = metrics.RequestDuationMetric.Min,
                    Stdev = metrics.RequestDuationMetric.StdDev,
                    Percentiles = new Dictionary<string, double>
                            {
                                {"50%", percentiles[0]},
                                {"75%", percentiles[1]},
                                {"95%", percentiles[2]},
                                {"99%", percentiles[3]},
                                {"99.9%", percentiles[4]},
                                {"99.99%", percentiles[5]},
                            }
                },
                Requests = new MeterData
                {
                    Count = metrics.ConcurrentRequests.Count,
                    FifteenMinuteRate = Math.Round(metrics.ConcurrentRequests.FifteenMinuteRate, 3),
                    FiveMinuteRate = Math.Round(metrics.ConcurrentRequests.FiveMinuteRate, 3),
                    MeanRate = Math.Round(metrics.ConcurrentRequests.MeanRate, 3),
                    OneMinuteRate = Math.Round(metrics.ConcurrentRequests.OneMinuteRate, 3),
                }
            };
        }
    }
}
