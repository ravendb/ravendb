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
using System.Globalization;
using System.IO;
using System.Linq;
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
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Database.Plugins.Catalogs;

namespace Raven.Database
{
	public class DocumentDatabase : IResourceStore, IDisposable
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private static string buildVersion;

		private static string productVersion;

		private readonly TaskScheduler backgroundTaskScheduler;

		private readonly ThreadLocal<bool> disableAllTriggers = new ThreadLocal<bool>(() => false);

		private readonly object idleLocker = new object();

		private readonly InFlightTransactionalState inFlightTransactionalState;

		private readonly IndexingExecuter indexingExecuter;

		private readonly LastCollectionEtags lastCollectionEtags;

		private readonly Prefetcher prefetcher;

		private readonly SequentialUuidGenerator uuidGenerator;

		private readonly List<IDisposable> toDispose = new List<IDisposable>();

		private readonly TransportState transportState;

		private readonly WorkContext workContext;

		private volatile bool backgroundWorkersSpun;

		private volatile bool disposed;

		private readonly DocumentDatabaseInitializer initializer;

		private readonly SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches;

		private readonly CancellationTokenSource _tpCts = new CancellationTokenSource();
		public DocumentDatabase(InMemoryRavenConfiguration configuration, DocumentDatabase systemDatabase, TransportState recievedTransportState = null)
		{
			TimerManager = new ResourceTimerManager();
			DocumentLock = new PutSerialLock();
			Name = configuration.DatabaseName;
			ResourceName = Name;
			Configuration = configuration;
			transportState = recievedTransportState ?? new TransportState();
			ExtensionsState = new AtomicDictionary<object>();
			
			using (LogManager.OpenMappedContext("database", Name ?? Constants.SystemDatabase))
			{
				Log.Debug("Start loading the following database: {0}", Name ?? Constants.SystemDatabase);

				initializer = new DocumentDatabaseInitializer(this, configuration);
				initializer.ValidateStorage();

				initializer.InitializeEncryption();
				initializer.ValidateLicense();

				initializer.SubscribeToDomainUnloadOrProcessExit();
				initializer.ExecuteAlterConfiguration();
				initializer.SatisfyImportsOnce();

				backgroundTaskScheduler = configuration.CustomTaskScheduler ?? TaskScheduler.Default;


				recentTouches = new SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo>(configuration.MaxRecentTouchesToRemember, StringComparer.OrdinalIgnoreCase);

				configuration.Container.SatisfyImportsOnce(this);

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
					initializer.InitializeTransactionalStorage(uuidGenerator);
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
                    initializer.InitializeIndexDefinitionStorage();
					Indexes = new IndexActions(this, recentTouches, uuidGenerator, Log);
                    Attachments = new AttachmentActions(this, recentTouches, uuidGenerator, Log);
					Maintenance = new MaintenanceActions(this, recentTouches, uuidGenerator, Log);
					Notifications = new NotificationActions(this, recentTouches, uuidGenerator, Log);
					Subscriptions = new SubscriptionActions(this, Log);
					Patches = new PatchActions(this, recentTouches, uuidGenerator, Log);
					Queries = new QueryActions(this, recentTouches, uuidGenerator, Log);
					Tasks = new TaskActions(this, recentTouches, uuidGenerator, Log);
					Transformers = new TransformerActions(this, recentTouches, uuidGenerator, Log);
                    Documents = new DocumentActions(this, recentTouches, uuidGenerator, Log);

                    ConfigurationRetriever = new ConfigurationRetriever(systemDatabase ?? this, this);

					inFlightTransactionalState = TransactionalStorage.GetInFlightTransactionalState(this, Documents.Put, Documents.Delete);

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
					InitializeTriggersExceptIndexCodecs();

					

					ExecuteStartupTasks();
					lastCollectionEtags.InitializeBasedOnIndexingResults();
					ReducingExecuter = new ReducingExecuter(workContext, IndexReplacer);

					

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

		public event EventHandler Disposing;

		public event EventHandler DisposingEnded;

		public event EventHandler StorageInaccessible;

		public event Action OnIndexingWiringComplete;

		public event Action<DocumentDatabase> OnBackupComplete;

		public static string BuildVersion
		{
			get { return buildVersion ?? (buildVersion = GetBuildVersion().ToString(CultureInfo.InvariantCulture)); }
		}

		public static string ProductVersion
		{
			get
			{
				if (!string.IsNullOrEmpty(productVersion))
				{
					return productVersion;
				}

				productVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<DocumentDatabase>()).ProductVersion;
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

		[Obsolete("Use RavenFS instead.")]
		public AttachmentActions Attachments { get; private set; }

		public TaskScheduler BackgroundTaskScheduler
		{
			get { return backgroundTaskScheduler; }
		}

		public InMemoryRavenConfiguration Configuration { get; private set; }

		public ConfigurationRetriever ConfigurationRetriever { get; private set; }

		[ImportMany]
		public OrderedPartCollection<AbstractDeleteTrigger> DeleteTriggers { get; set; }

		/// <summary>
		///     Whatever this database has been disposed
		/// </summary>
		public bool Disposed
		{
			get { return disposed; }
		}

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
		public InFlightTransactionalState InFlightTransactionalState
		{
			get { return inFlightTransactionalState; }
		}

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
		public IndexingExecuter IndexingExecuter
		{
			get { return indexingExecuter; }
		}

		public LastCollectionEtags LastCollectionEtags
		{
			get { return lastCollectionEtags; }
		}

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

		public Prefetcher Prefetcher
		{
			get { return prefetcher; }
		}

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

			return types
				.SelectMany(type => unfilteredAggregate.GetExports(new ImportDefinition(info => info.Metadata.ContainsKey("Bundle"), type.FullName, ImportCardinality.ZeroOrMore, false, false)))
				.Select(info => info.Item2.Metadata["Bundle"] as string)
				.Where(x => x != null)
				.Distinct()
				.OrderBy(x => x)
				.ToList();
		}

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

					result.Indexes = actions.Indexing.GetIndexesStats().Where(x => x != null).Select(x =>
		{
			Index indexInstance = IndexStorage.GetIndexInstance(x.Id);
			if (indexInstance == null)
				return null;
			x.Name = indexInstance.PublicName;
			x.SetLastDocumentEtag(result.LastDocEtag);
			return x;
		})
						.Where(x => x != null)
						.ToArray();
				});

				if (result.Indexes != null)
				{
					foreach (IndexStats index in result.Indexes)
					{
						try
						{
							IndexDefinition indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index.Id);
							index.LastQueryTimestamp = IndexStorage.GetLastQueryTime(index.Id);
							index.Performance = IndexStorage.GetIndexingPerformance(index.Id);
							index.IsTestIndex = indexDefinition.IsTestIndex;
							index.IsOnRam = IndexStorage.IndexOnRam(index.Id);
							index.LockMode = indexDefinition.LockMode;

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

				return result;
			}
		}

		public TaskActions Tasks { get; private set; }

		[CLSCompliant(false)]
		public ITransactionalStorage TransactionalStorage { get; private set; }

		[CLSCompliant(false)]
		public TransformerActions Transformers { get; private set; }

		public TransportState TransportState
		{
			get { return transportState; }
		}

		public WorkContext WorkContext
		{
			get { return workContext; }
		}

		public BatchResult[] Batch(IList<ICommandData> commands, CancellationToken token)
		{
			using (DocumentLock.Lock())
			{
				bool shouldRetryIfGotConcurrencyError = commands.All(x => ((x is PatchCommandData || IsScriptedPatchCommandDataWithoutEtagProperty(x)) && (x.Etag == null)));
				if (shouldRetryIfGotConcurrencyError)
				{
					Stopwatch sp = Stopwatch.StartNew();
					BatchResult[] result = BatchWithRetriesOnConcurrencyErrorsAndNoTransactionMerging(commands, token);
					Log.Debug("Successfully executed {0} patch commands in {1}", commands.Count, sp.Elapsed);
					return result;
				}

				BatchResult[] results = null;
				TransactionalStorage.Batch(
					actions => { results = ProcessBatch(commands, token); });

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
		public IDisposable DisableAllTriggersForCurrentThread()
		{
			if (disposed)
				return new DisposableAction(() => { });

			bool old = disableAllTriggers.Value;
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

		public void Dispose()
		{
			if (disposed)
				return;

			Log.Debug("Start shutdown the following database: {0}", Name ?? Constants.SystemDatabase);

			EventHandler onDisposing = Disposing;
			_tpCts.Cancel();
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
								if (prefetcher != null)
									prefetcher.Dispose();
							});

			exceptionAggregator.Execute(() =>
							{
								initializer.UnsubscribeToDomainUnloadOrProcessExit();
								disposed = true;

								if (workContext != null)
									workContext.StopWorkRude();
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
				StopMappingThreadPool();
			});
			exceptionAggregator.Execute(() =>
			{
				StopReducingThreadPool();
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
				exceptionAggregator.Execute(TransactionalStorage.Dispose);

			if (Configuration != null)
				exceptionAggregator.Execute(Configuration.Dispose);

			exceptionAggregator.Execute(disableAllTriggers.Dispose);

			if (workContext != null)
				exceptionAggregator.Execute(workContext.Dispose);

			if (TimerManager != null)
				exceptionAggregator.Execute(TimerManager.Dispose);

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

		public RavenThreadPool MappingThreadPool;
		public RavenThreadPool ReducingThreadPool;

		public void SpinBackgroundWorkers()
		{
			if (backgroundWorkersSpun)
				throw new InvalidOperationException("The background workers has already been spun and cannot be spun again");
			var disableIndexing = Configuration.Settings[Constants.IndexingDisabled];
			if (null != disableIndexing)
			{
				bool disableIndexingStatus;
				var res = bool.TryParse(disableIndexing, out disableIndexingStatus);
				if (res && disableIndexingStatus) return; //indexing were set to disable 
			}
			backgroundWorkersSpun = true;

			workContext.StartWork();

			MappingThreadPool = new RavenThreadPool(Configuration.MaxNumberOfParallelProcessingTasks * 2, _tpCts.Token, "Map Thread Pool", new[]
					{
						new Action(()=> indexingExecuter.Execute())
					});
			ReducingThreadPool = new RavenThreadPool(Configuration.MaxNumberOfParallelProcessingTasks * 2, _tpCts.Token, "Reduce Thread Pool", new[]
					{
						new Action(()=>ReducingExecuter.Execute())
					});

			MappingThreadPool.Start();
			ReducingThreadPool.Start();

			RaiseIndexingWiringComplete();

		}

		private void StopMappingThreadPool()
		{
			if (MappingThreadPool != null)
			{
				MappingThreadPool.WaitForWorkToBeDone();
				MappingThreadPool.Dispose();
				MappingThreadPool = null;
			}
			MappingThreadPool = null;
		}

		private void StopReducingThreadPool()
		{
			if (ReducingThreadPool != null)
			{
				ReducingThreadPool.WaitForWorkToBeDone();
				ReducingThreadPool.Dispose();
				ReducingThreadPool = null;
			}
		}

		public void StopBackgroundWorkers()
		{
			workContext.StopWork();
			StopMappingThreadPool();
			StopReducingThreadPool();

			backgroundWorkersSpun = false;
		}

		public void StopIndexingWorkers()
		{
			workContext.StopIndexing();
			try
			{
				StopMappingThreadPool();
			}
			catch (Exception e)
			{
				Log.WarnException("Error while trying to stop background indexing", e);
			}

			try
			{
				StopReducingThreadPool();
			}
			catch (Exception e)
			{
				Log.WarnException("Error while trying to stop background reducing", e);
			}

			backgroundWorkersSpun = false;
		}

		protected void RaiseIndexingWiringComplete()
		{
			Action indexingWiringComplete = OnIndexingWiringComplete;
			OnIndexingWiringComplete = null; // we can only init once, release all actions
			if (indexingWiringComplete != null)
				indexingWiringComplete();
		}

		private static int GetBuildVersion()
		{
			string location = AssemblyHelper.GetAssemblyLocationFor<DocumentDatabase>();

			FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(location);
			if (fileVersionInfo.FilePrivatePart != 0)
				return fileVersionInfo.FilePrivatePart;

			return fileVersionInfo.FileBuildPart;
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
			using (LogContext.WithDatabase(Name))
			{
				foreach (var task in StartupTasks)
				{
					var disposable = task.Value as IDisposable;
					if (disposable != null)
					{
						toDispose.Add(disposable);
					}

					task.Value.Execute(this);
				}
			}
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
			for (int index = 0; index < commands.Count; index++)
			{
				token.ThrowIfCancellationRequested();

				ICommandData command = commands[index];
				results[index] = command.ExecuteBatch(this);
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

			private ValidateLicense validateLicense;

			public DocumentDatabaseInitializer(DocumentDatabase database, InMemoryRavenConfiguration configuration)
			{
				this.database = database;
				this.configuration = configuration;
			}

			public void ValidateStorage()
			{
				var storageEngineTypeName = configuration.SelectStorageEngineAndFetchTypeName();
				if (InMemoryRavenConfiguration.VoronTypeName == storageEngineTypeName
					&& configuration.Storage.Voron.AllowOn32Bits == false && 
                    Environment.Is64BitProcess == false)
				{
					throw new Exception("Voron is prone to failure in 32-bits mode. Use " + Constants.Voron.AllowOn32Bits + " to force voron in 32-bit process.");
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
				foreach (IAlterConfiguration alterConfiguration in configuration.Container.GetExportedValues<IAlterConfiguration>())
				{
					alterConfiguration.AlterConfiguration(configuration);
				}
			}

			public void SatisfyImportsOnce()
			{
				configuration.Container.SatisfyImportsOnce(database);
			}

			public void InitializeTransactionalStorage(IUuidGenerator uuidGenerator)
			{
				string storageEngineTypeName = configuration.SelectStorageEngineAndFetchTypeName();
				database.TransactionalStorage = configuration.CreateTransactionalStorage(storageEngineTypeName, database.WorkContext.HandleWorkNotifications, () =>
							{
								if (database.StorageInaccessible != null)
									database.StorageInaccessible(database, EventArgs.Empty);

							});
				database.TransactionalStorage.Initialize(uuidGenerator, database.DocumentCodecs);
			}

		    public void InitializeIndexDefinitionStorage()
			{
				database.IndexDefinitionStorage = new IndexDefinitionStorage(configuration, database.TransactionalStorage, configuration.DataDirectory, database.Extensions);
		    }

			public void InitializeIndexStorage()
			{
				database.IndexStorage = new IndexStorage(database.IndexDefinitionStorage, configuration, database);
			}


		}

		public void RaiseBackupComplete()
		{
			var onOnBackupComplete = OnBackupComplete;
			if (onOnBackupComplete != null) onOnBackupComplete(this);
		}
	}
}
