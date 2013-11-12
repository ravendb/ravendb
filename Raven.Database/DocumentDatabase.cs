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
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Commercial;
using Raven.Database.Impl.DTC;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Prefetching;
using Raven.Database.Queries;
using Raven.Database.Server;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Responders.Debugging;
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
		public OrderedPartCollection<AbstractRequestResponder> RequestResponders { get; set; }

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
		private System.Threading.Tasks.Task indexingBackgroundTask;
		private System.Threading.Tasks.Task reducingBackgroundTask;
		private readonly TaskScheduler backgroundTaskScheduler;
		private readonly object idleLocker = new object();

		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches =
			new SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo>(1024, StringComparer.OrdinalIgnoreCase);

		public DocumentDatabase(InMemoryRavenConfiguration configuration, TransportState transportState = null)
		{
			this.configuration = configuration;
			this.transportState = transportState ?? new TransportState();
			InitializeEncryption(configuration);
			using (LogManager.OpenMappedContext("database", configuration.DatabaseName ?? Constants.SystemDatabase))
			{
				if (configuration.IsTenantDatabase == false)
				{
					validateLicense = new ValidateLicense();
					validateLicense.Execute(configuration);
				}
				AppDomain.CurrentDomain.DomainUnload += DomainUnloadOrProcessExit;
				AppDomain.CurrentDomain.ProcessExit += DomainUnloadOrProcessExit;

				Name = configuration.DatabaseName;
				backgroundTaskScheduler = configuration.CustomTaskScheduler ?? TaskScheduler.Current;

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
					RaiseIndexChangeNotification = RaiseNotifications,
					TaskScheduler = backgroundTaskScheduler,
					Configuration = configuration,
					IndexReaderWarmers = IndexReaderWarmers
				};

				TransactionalStorage = configuration.CreateTransactionalStorage(workContext.HandleWorkNotifications);

				try
				{
					sequentialUuidGenerator = new SequentialUuidGenerator();
					TransactionalStorage.Initialize(sequentialUuidGenerator, DocumentCodecs);
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

					InitializeTriggersExceptIndexCodecs();
					SecondStageInitialization();

					ExecuteStartupTasks();
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
			Lucene.Net.Support.Cryptography.FIPSCompliant = configuration.UseFips;
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

			workContext.Init(Name);
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
							pendingTaskAndState.Task.Wait();
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
			reducingBackgroundTask = Task.Factory.StartNew(
				new ReducingExecuter(workContext).Execute,
				CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
		}

		public void SpinIndexingWorkers()
		{
			if (backgroundWorkersSpun)
				throw new InvalidOperationException("The background workers has already been spun and cannot be spun again");

			backgroundWorkersSpun = true;

			workContext.StartIndexing();
			indexingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
				indexingExecuter.Execute,
				CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
			reducingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
				new ReducingExecuter(workContext).Execute,
				CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
		}

		public void RaiseNotifications(DocumentChangeNotification obj, RavenJObject metadata)
		{
			TransportState.Send(obj);
			var onDocumentChange = OnDocumentChange;
			if (onDocumentChange != null)
				onDocumentChange(this, obj, metadata);
		}

		public void RaiseNotifications(IndexChangeNotification obj)
		{
			TransportState.Send(obj);
		}

		public void RaiseNotifications(ReplicationConflictNotification obj)
		{
			TransportState.Send(obj);
		}

		public void RaiseNotifications(BulkInsertChangeNotification obj)
		{
			TransportState.Send(obj);
		}

		public event Action<DocumentDatabase, DocumentChangeNotification, RavenJObject> OnDocumentChange;

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
			}
			finally
			{
				if (tryEnter)
					Monitor.Exit(idleLocker);
			}
		}

		private void ClearCompletedPendingTasks()
		{
			foreach (var taskAndState in pendingTasks)
			{
				var task = taskAndState.Value.Task;
				if (task.IsCompleted || task.IsCanceled || task.IsFaulted)
				{
					PendingTaskAndState value;
					pendingTasks.TryRemove(taskAndState.Key, out value);
				}
				if (task.Exception != null)
				{
					log.InfoException("Failed to execute background task " + taskAndState.Key, task.Exception);
				}
			}
		}

		public JsonDocument Get(string key, TransactionInformation transactionInformation)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();

			JsonDocument document = null;
			if (transactionInformation == null ||
				inFlightTransactionalState.TryGet(key, transactionInformation, out document) == false)
			{
				// first we check the dtc state, then the storage, to avoid race conditions
				var nonAuthoritativeInformationBehavior = inFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(transactionInformation, key);

				TransactionalStorage.Batch(actions => { document = actions.Documents.DocumentByKey(key, transactionInformation); });

				if (nonAuthoritativeInformationBehavior != null)
					document = nonAuthoritativeInformationBehavior(document);
			}

			DocumentRetriever.EnsureIdInMetadata(document);

			return new DocumentRetriever(null, ReadTriggers, inFlightTransactionalState)
				.ExecuteReadTriggers(document, transactionInformation, ReadOperation.Load);
		}

		public JsonDocumentMetadata GetDocumentMetadata(string key, TransactionInformation transactionInformation)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();
			JsonDocumentMetadata document = null;
			if (transactionInformation == null ||
				inFlightTransactionalState.TryGet(key, transactionInformation, out document) == false)
			{
				var nonAuthoritativeInformationBehavior = inFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocumentMetadata>(transactionInformation, key);
				TransactionalStorage.Batch(actions =>
				{
					document = actions.Documents.DocumentMetadataByKey(key, transactionInformation);
				});
				if (nonAuthoritativeInformationBehavior != null)
					document = nonAuthoritativeInformationBehavior(document);
			}

			DocumentRetriever.EnsureIdInMetadata(document);
			return new DocumentRetriever(null, ReadTriggers, inFlightTransactionalState)
				.ProcessReadVetoes(document, transactionInformation, ReadOperation.Load);
		}


		public void PutDocumentMetadata(string key, RavenJObject metadata)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();
			TransactionalStorage.Batch(actions =>
			{
				actions.Documents.PutDocumentMetadata(key, metadata);
				workContext.ShouldNotifyAboutWork(() => "PUT (metadata) " + key);
			});
		}

		public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			workContext.DocsPerSecIncreaseBy(1);
			key = string.IsNullOrWhiteSpace(key) ? Guid.NewGuid().ToString() : key.Trim();
			RemoveReservedProperties(document);
			RemoveMetadataReservedProperties(metadata);
			Etag newEtag = Etag.Empty;

			using (TransactionalStorage.WriteLock())
			{
				TransactionalStorage.Batch(actions =>
				{
					if (key.EndsWith("/"))
					{
						key += GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions,
																						 transactionInformation);
					}
					AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
					if (transactionInformation == null)
					{
						if (inFlightTransactionalState.IsModified(key))
							throw new ConcurrencyException("PUT attempted on : " + key +
														   " while it is being locked by another transaction");

						PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, null));

						var addDocumentResult = actions.Documents.AddDocument(key, etag, document, metadata);
						newEtag = addDocumentResult.Etag;

						CheckReferenceBecauseOfDocumentUpdate(key, actions);
						metadata[Constants.LastModified] = addDocumentResult.SavedAt;
						metadata.EnsureSnapshot(
							"Metadata was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");
						document.EnsureSnapshot(
							"Document was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");

						actions.AfterStorageCommitBeforeWorkNotifications(new JsonDocument
						{
							Metadata = metadata,
							Key = key,
							DataAsJson = document,
							Etag = newEtag,
							LastModified = addDocumentResult.SavedAt,
							SkipDeleteFromIndex = addDocumentResult.Updated == false
						}, documents =>
						{
							SetPerCollectionEtags(documents);
							etagSynchronizer.UpdateSynchronizationState(documents);
							prefetcher.GetPrefetchingBehavior(PrefetchingUser.Indexer, null).AfterStorageCommitBeforeWorkNotifications(documents);
						});

						if (addDocumentResult.Updated)
							prefetcher.AfterUpdate(key, addDocumentResult.PrevEtag);

						PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, newEtag, null));

						TransactionalStorage
							.ExecuteImmediatelyOrRegisterForSynchronization(() =>
							{
								PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata, newEtag));
								RaiseNotifications(new DocumentChangeNotification
								{
									Id = key,
									Type = DocumentChangeTypes.Put,
									Etag = newEtag,
								}, metadata);
							});

						workContext.ShouldNotifyAboutWork(() => "PUT " + key);
					}
					else
					{
						var doc = actions.Documents.DocumentMetadataByKey(key, null);
						newEtag = inFlightTransactionalState.AddDocumentInTransaction(key, etag, document, metadata,
																					  transactionInformation,
																					  doc == null
																						  ? Etag.Empty
																						  : doc.Etag,
																					  sequentialUuidGenerator);
					}
				});

				log.Debug("Put document {0} with etag {1}", key, newEtag);
				return new PutResult
				{
					Key = key,
					ETag = newEtag
				};
			}
		}

		private void SetPerCollectionEtags(JsonDocument[] documents)
		{
			var collections = documents.GroupBy(x => x.Metadata[Constants.RavenEntityName])
					 .Where(x => x.Key != null)
					 .Select(x => new { Etag = x.Max(y => y.Etag), CollectionName = x.Key.ToString() })
					 .ToArray();

			TransactionalStorage.Batch(accessor =>
			{
				foreach (var collection in collections)
					SetLastEtagForCollection(accessor, collection.CollectionName, collection.Etag);
			});
		}

		private void SetLastEtagForCollection(IStorageActionsAccessor actions, string collectionName, Etag etag)
		{
			actions.Lists.Set("Raven/Collection/Etag", collectionName, RavenJObject.FromObject(new
			{
				Etag = etag.ToByteArray()
			}), UuidType.Documents);
		}

		public Etag GetLastEtagForCollection(string collectionName)
		{
			Etag value = Etag.Empty;
			TransactionalStorage.Batch(accessor =>
			{
				var dbvalue = accessor.Lists.Read("Raven/Collection/Etag", collectionName);
				if (dbvalue != null)
				{
					value = Etag.Parse(dbvalue.Data.Value<Byte[]>("Etag"));
				}
			});
			return value;
		}

		internal void CheckReferenceBecauseOfDocumentUpdate(string key, IStorageActionsAccessor actions)
		{
			TouchedDocumentInfo touch;
			recentTouches.TryRemove(key, out touch);

			foreach (var referencing in actions.Indexing.GetDocumentsReferencing(key))
			{
				Etag preTouchEtag;
				Etag afterTouchEtag;
				actions.Documents.TouchDocument(referencing, out preTouchEtag, out afterTouchEtag);
				if (preTouchEtag == null || afterTouchEtag == null)
					continue;

				actions.General.MaybePulseTransaction();

				recentTouches.Set(referencing, new TouchedDocumentInfo
				{
					PreTouchEtag = preTouchEtag,
					TouchedEtag = afterTouchEtag
				});
			}
		}

		public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
			IStorageActionsAccessor actions,
			TransactionInformation transactionInformation)
		{
			int tries;
			return GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions, transactionInformation, out tries);
		}

		public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
			IStorageActionsAccessor actions,
			TransactionInformation transactionInformation,
			out int tries)
		{
			long nextIdentityValue = actions.General.GetNextIdentityValue(key);

			if (actions.Documents.DocumentMetadataByKey(key + nextIdentityValue, transactionInformation) == null)
			{
				tries = 1;
				return nextIdentityValue;
			}
			tries = 1;
			// there is already a document with this id, this means that we probably need to search
			// for an opening in potentially large data set. 
			var lastKnownBusy = nextIdentityValue;
			var maybeFree = nextIdentityValue * 2;
			var lastKnownFree = long.MaxValue;
			while (true)
			{
				tries++;
				if (actions.Documents.DocumentMetadataByKey(key + maybeFree, transactionInformation) == null)
				{
					if (lastKnownBusy + 1 == maybeFree)
					{
						actions.General.SetIdentityValue(key, maybeFree);
						return maybeFree;
					}
					lastKnownFree = maybeFree;
					maybeFree = Math.Max(maybeFree - (maybeFree - lastKnownBusy) / 2, lastKnownBusy + 1);

				}
				else
				{
					lastKnownBusy = maybeFree;
					maybeFree = Math.Min(lastKnownFree, maybeFree * 2);
				}
			}
		}

		private void AssertPutOperationNotVetoed(string key, RavenJObject metadata, RavenJObject document, TransactionInformation transactionInformation)
		{
			var vetoResult = PutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, document, metadata, transactionInformation) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertAttachmentPutOperationNotVetoed(string key, RavenJObject metadata, Stream data)
		{
			var vetoResult = AttachmentPutTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, data, metadata) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("PUT vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertAttachmentDeleteOperationNotVetoed(string key)
		{
			var vetoResult = AttachmentDeleteTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("DELETE vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private void AssertDeleteOperationNotVetoed(string key, TransactionInformation transactionInformation)
		{
			var vetoResult = DeleteTriggers
				.Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key, transactionInformation) })
				.FirstOrDefault(x => x.VetoResult.IsAllowed == false);
			if (vetoResult != null)
			{
				throw new OperationVetoedException("DELETE vetoed by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
			}
		}

		private static void RemoveMetadataReservedProperties(RavenJObject metadata)
		{
			RemoveReservedProperties(metadata);
			metadata.Remove("Raven-Last-Modified");
			metadata.Remove("Last-Modified");
		}

		private static void RemoveReservedProperties(RavenJObject document)
		{
			document.Remove(string.Empty);
			var toRemove = document.Keys.Where(propertyName => propertyName.StartsWith("@") || headersToIgnoreServer.Contains(propertyName)).ToList();
			foreach (var propertyName in toRemove)
			{
				document.Remove(propertyName);
			}
		}

		private static readonly HashSet<string> headersToIgnoreServer = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
		{
			Constants.RavenLastModified,
		};

		public bool Delete(string key, Etag etag, TransactionInformation transactionInformation)
		{
			RavenJObject metadata;
			return Delete(key, etag, transactionInformation, out metadata);
		}

		public bool Delete(string key, Etag etag, TransactionInformation transactionInformation, out RavenJObject metadata)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();

			var deleted = false;
			log.Debug("Delete a document with key: {0} and etag {1}", key, etag);
			RavenJObject metadataVar = null;
			using (TransactionalStorage.WriteLock())
			{
				TransactionalStorage.Batch(actions =>
				{
					AssertDeleteOperationNotVetoed(key, transactionInformation);
					if (transactionInformation == null)
					{
						DeleteTriggers.Apply(trigger => trigger.OnDelete(key, null));

						Etag deletedETag;
						if (actions.Documents.DeleteDocument(key, etag, out metadataVar, out deletedETag))
						{
							deleted = true;
							actions.Indexing.RemoveAllDocumentReferencesFrom(key);
							WorkContext.MarkDeleted(key);

							CheckReferenceBecauseOfDocumentUpdate(key, actions);

							foreach (var indexName in IndexDefinitionStorage.IndexNames)
							{
								AbstractViewGenerator abstractViewGenerator =
									IndexDefinitionStorage.GetViewGenerator(indexName);
								if (abstractViewGenerator == null)
									continue;

								var token = metadataVar.Value<string>(Constants.RavenEntityName);

								if (token != null && // the document has a entity name
									abstractViewGenerator.ForEntityNames.Count > 0)
								// the index operations on specific entities
								{
									if (abstractViewGenerator.ForEntityNames.Contains(token) == false)
										continue;
								}

								var instance = IndexDefinitionStorage.GetIndexDefinition(indexName);
								var task = actions.GetTask(x => x.Index == instance.IndexId, new RemoveFromIndexTask
								{
									Index = instance.IndexId
								});
								task.Keys.Add(key);
							}
							if (deletedETag != null)
								prefetcher.AfterDelete(key, deletedETag);
							DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, null));
						}

						TransactionalStorage
							.ExecuteImmediatelyOrRegisterForSynchronization(() =>
							{
								DeleteTriggers.Apply(trigger => trigger.AfterCommit(key));
								RaiseNotifications(new DocumentChangeNotification
								{
									Id = key,
									Type = DocumentChangeTypes.Delete,
								}, metadataVar);
							});

					}
					else
					{
						var doc = actions.Documents.DocumentMetadataByKey(key, null);

						inFlightTransactionalState.DeleteDocumentInTransaction(transactionInformation, key,
																			   etag,
																			   doc == null ? Etag.Empty : doc.Etag,
																			   sequentialUuidGenerator);
						deleted = doc != null;
					}

					workContext.ShouldNotifyAboutWork(() => "DEL " + key);
				});

				metadata = metadataVar;
				return deleted;
			}
		}
		public bool HasTransaction(string txId)
		{
			return inFlightTransactionalState.HasTransaction(txId);
		}

		public void PrepareTransaction(string txId)
		{
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

		[MethodImpl(MethodImplOptions.Synchronized)]
		public string PutTransform(string name, TransformerDefinition definition)
		{
			if (name == null) throw new ArgumentNullException("name");
			if (definition == null) throw new ArgumentNullException("definition");

			name = name.Trim();

			var existingDefinition = IndexDefinitionStorage.GetTransformerDefinition(name);
			if (existingDefinition != null && existingDefinition.Equals(definition))
				return name; // no op for the same transformer

			IndexDefinitionStorage.CreateAndPersistTransform(definition);
			IndexDefinitionStorage.AddTransform(definition.IndexId, definition);

			return name;
		}

		// only one index can be created at any given time
		// the method already handle attempts to create the same index, so we don't have to 
		// worry about this.
		[MethodImpl(MethodImplOptions.Synchronized)]
		public string PutIndex(string name, IndexDefinition definition)
		{
			if (name == null)
				throw new ArgumentNullException("name");

			var existingIndex = IndexDefinitionStorage.GetIndexDefinition(name);

			if (existingIndex != null)
			{
				switch (existingIndex.LockMode)
				{
					case IndexLockMode.LockedIgnore:
						log.Info("Index {0} not saved because it was lock (with ignore)", name);
						return name;

					case IndexLockMode.LockedError:
						throw new InvalidOperationException("Can not overwrite locked index: " + name);
				}
			}

			name = name.Trim();

			switch (FindIndexCreationOptions(definition, ref name))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					// ensure that the code can compile
					new DynamicViewCompiler(definition.Name, definition, Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Configuration).GenerateInstance();
					DeleteIndex(name);
					break;
			}



			TransactionalStorage.Batch(actions =>
			{
				definition.IndexId = (int)GetNextIdentityValueWithoutOverwritingOnExistingDocuments("IndexId", actions, null);
				IndexDefinitionStorage.RegisterNewIndexInThisSession(name, definition);

				// this has to happen in this fashion so we will expose the in memory status after the commit, but 
				// before the rest of the world is notified about this.

				IndexDefinitionStorage.CreateAndPersistIndex(definition);
				IndexStorage.CreateIndexImplementation(definition);

				InvokeSuggestionIndexing(name, definition);

				actions.Indexing.AddIndex(definition.IndexId, definition.IsMapReduce);
				workContext.ShouldNotifyAboutWork(() => "PUT INDEX " + name);
			});

			// The act of adding it here make it visible to other threads
			// we have to do it in this way so first we prepare all the elements of the 
			// index, then we add it to the storage in a way that make it public
			IndexDefinitionStorage.AddIndex(definition.IndexId, definition);

			workContext.ClearErrorsFor(name);

			TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => RaiseNotifications(new IndexChangeNotification
			{
				Name = name,
				Type = IndexChangeTypes.IndexAdded,
			}));

			return name;
		}

		private void InvokeSuggestionIndexing(string name, IndexDefinition definition)
		{
			foreach (var suggestion in definition.Suggestions)
			{
				var field = suggestion.Key;
				var suggestionOption = suggestion.Value;

				if (suggestionOption.Distance == StringDistanceTypes.None)
					continue;

				var indexExtensionKey =
					MonoHttpUtility.UrlEncode(field + "-" + suggestionOption.Distance + "-" +
											  suggestionOption.Accuracy);

				var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
					 workContext,
					 Path.Combine(configuration.IndexStoragePath, "Raven-Suggestions", name, indexExtensionKey),
					 SuggestionQueryRunner.GetStringDistance(suggestionOption.Distance),
					 configuration.RunInMemory,
					 field,
					 suggestionOption.Accuracy);

				IndexStorage.SetIndexExtension(name, indexExtensionKey, suggestionQueryIndexExtension);
			}
		}

		private IndexCreationOptions FindIndexCreationOptions(IndexDefinition definition, ref string name)
		{
			definition.Name = name;
			definition.RemoveDefaultValues();
			IndexDefinitionStorage.ResolveAnalyzers(definition);
			var findIndexCreationOptions = IndexDefinitionStorage.FindIndexCreationOptions(definition);
			return findIndexCreationOptions;
		}

		public QueryResultWithIncludes Query(string index, IndexQuery query)
		{
			QueryResultWithIncludes result = null;
			TransactionalStorage.Batch(accessor =>
			{
				using (var op = new DatabaseQueryOperation(this, index, query, accessor)
				{
					ShouldSkipDuplicateChecking = query.SkipDuplicateChecking
				})
				{
					var list = new List<RavenJObject>();
					op.Init();
					op.Execute(list.Add);
					op.Result.Results = list;
					result = op.Result;
				}
			});
			return result;
		}

		public QueryResultWithIncludes Query(string indexName, IndexQuery query, Action<QueryHeaderInformation> headerInfo, Action<RavenJObject> onResult)
		{
			var queryStat = AddToCurrentlyRunningQueryList(indexName, query);

			try
			{
				indexName = indexName != null ? indexName.Trim() : null;
				var highlightings = new Dictionary<string, Dictionary<string, string[]>>();
				var scoreExplanations = new Dictionary<string, string>();
				Func<IndexQueryResult, object> tryRecordHighlightingAndScoreExplanation = queryResult =>
				{
					if (queryResult.Key == null)
						return null;
					if (queryResult.Highligtings != null)
						highlightings.Add(queryResult.Key, queryResult.Highligtings);
					if (queryResult.ScoreExplanation != null)
						scoreExplanations.Add(queryResult.Key, queryResult.ScoreExplanation);
					return null;
				};
				var stale = false;
				System.Tuple<DateTime, Etag> indexTimestamp = Tuple.Create(DateTime.MinValue, Etag.Empty);
				Etag resultEtag = Etag.Empty;
				var nonAuthoritativeInformation = false;

				if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
				{
					query.FieldsToFetch = new[] { Constants.AllFields };
				}


				var duration = Stopwatch.StartNew();
				var idsToLoad = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
				TransactionalStorage.Batch(
					actions =>
					{
						var viewGenerator = IndexDefinitionStorage.GetViewGenerator(indexName);
						var index = IndexDefinitionStorage.GetIndexDefinition(indexName);
						if (viewGenerator == null)
							throw new IndexDoesNotExistsException("Could not find index named: " + indexName);

						resultEtag = GetIndexEtag(index.Name, null, query.ResultsTransformer);

						stale = actions.Staleness.IsIndexStale(index.IndexId, query.Cutoff, lastCollectionEtags.OptimizeCutoffForIndex(viewGenerator, query.CutoffEtag));

						if (stale == false && query.Cutoff == null && query.CutoffEtag == null)
						{
							var indexInstance = IndexStorage.GetIndexInstance(indexName);
							stale = stale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
						}
						indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index.IndexId);
						var indexFailureInformation = actions.Indexing.GetFailureRate(index.IndexId);
						if (indexFailureInformation.IsInvalidIndex)
						{
							throw new IndexDisabledException(indexFailureInformation);
						}
						var docRetriever = new DocumentRetriever(actions, ReadTriggers, inFlightTransactionalState, query.QueryInputs, idsToLoad);
						var fieldsToFetch = new FieldsToFetch(query.FieldsToFetch, query.IsDistinct,
															  viewGenerator.ReduceDefinition == null
																? Constants.DocumentIdFieldName
																: Constants.ReduceKeyFieldName);
						Func<IndexQueryResult, bool> shouldIncludeInResults =
							result => docRetriever.ShouldIncludeResultInQuery(result, index, fieldsToFetch, query.SkipDuplicateChecking);
						var indexQueryResults = IndexStorage.Query(indexName, query, shouldIncludeInResults, fieldsToFetch, IndexQueryTriggers);
						indexQueryResults = new ActiveEnumerable<IndexQueryResult>(indexQueryResults);

						var transformerErrors = new List<string>();
						var results = GetQueryResults(query, viewGenerator, docRetriever,
													  from queryResult in indexQueryResults
													  let doc = docRetriever.RetrieveDocumentForQuery(queryResult, index, fieldsToFetch, query.SkipDuplicateChecking)
													  where doc != null
													  let _ = nonAuthoritativeInformation |= (doc.NonAuthoritativeInformation ?? false)
													  let __ = tryRecordHighlightingAndScoreExplanation(queryResult)
													  select doc, transformerErrors);

						if (headerInfo != null)
						{
							headerInfo(new QueryHeaderInformation
							{
								Index = indexName,
								IsStable = stale,
								ResultEtag = resultEtag,
								IndexTimestamp = indexTimestamp.Item1,
								IndexEtag = indexTimestamp.Item2,
								TotalResults = query.TotalSize.Value
							});
						}
						using (new CurrentTransformationScope(docRetriever))
						{
							foreach (var result in results)
							{
								onResult(result);
							}
							if (transformerErrors.Count > 0)
							{
								throw new InvalidOperationException("The transform results function failed.\r\n" + string.Join("\r\n", transformerErrors));
							}

						}


					});

				return new QueryResultWithIncludes
				{
					IndexName = indexName,
					IsStale = stale,
					NonAuthoritativeInformation = nonAuthoritativeInformation,
					SkippedResults = query.SkippedResults.Value,
					TotalResults = query.TotalSize.Value,
					IndexTimestamp = indexTimestamp.Item1,
					IndexEtag = indexTimestamp.Item2,
					ResultEtag = resultEtag,
					IdsToInclude = idsToLoad,
					LastQueryTime = SystemTime.UtcNow,
					Highlightings = highlightings,
					DurationMilliseconds = duration.ElapsedMilliseconds,
					ScoreExplanations = scoreExplanations
				};
			}
			finally
			{
				RemoveFromCurrentlyRunningQueryList(indexName, queryStat);
			}
		}

		public class DatabaseQueryOperation : IDisposable
		{
			public bool ShouldSkipDuplicateChecking = false;
			private readonly DocumentDatabase database;
			private readonly string indexName;
			private readonly IndexQuery query;
			private readonly IStorageActionsAccessor actions;
			private readonly ExecutingQueryInfo queryStat;
			public QueryResultWithIncludes Result = new QueryResultWithIncludes();
			public QueryHeaderInformation Header;
			private bool stale;
			private IEnumerable<RavenJObject> results;
			private DocumentRetriever docRetriever;
			private Stopwatch duration;
			private List<string> transformerErrors;
			private bool nonAuthoritativeInformation;
			private Etag resultEtag;
			private Tuple<DateTime, Etag> indexTimestamp;
			private Dictionary<string, Dictionary<string, string[]>> highlightings;
			private Dictionary<string, string> scoreExplanations;
			private HashSet<string> idsToLoad;

			public DatabaseQueryOperation(DocumentDatabase database, string indexName, IndexQuery query, IStorageActionsAccessor actions)
			{
				this.database = database;
				this.indexName = indexName != null ? indexName.Trim() : null;
				this.query = query;
				this.actions = actions;
				queryStat = database.AddToCurrentlyRunningQueryList(indexName, query);
			}

			public void Init()
			{
				highlightings = new Dictionary<string, Dictionary<string, string[]>>();
				scoreExplanations = new Dictionary<string, string>();
				Func<IndexQueryResult, object> tryRecordHighlightingAndScoreExplanation = queryResult =>
				{
					if (queryResult.Key == null)
						return null;
					if (queryResult.Highligtings != null)
						highlightings.Add(queryResult.Key, queryResult.Highligtings);
					if (queryResult.ScoreExplanation != null)
						scoreExplanations.Add(queryResult.Key, queryResult.ScoreExplanation);
					return null;
				};
				stale = false;
				indexTimestamp = Tuple.Create(DateTime.MinValue, Etag.Empty);
				resultEtag = Etag.Empty;
				nonAuthoritativeInformation = false;

				if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
				{
					query.FieldsToFetch = new[] { Constants.AllFields };
				}

				duration = Stopwatch.StartNew();
				idsToLoad = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

				var viewGenerator = database.IndexDefinitionStorage.GetViewGenerator(indexName);
				var index = database.IndexDefinitionStorage.GetIndexDefinition(indexName);
				if (viewGenerator == null)
					throw new IndexDoesNotExistsException("Could not find index named: " + indexName);

				resultEtag = database.GetIndexEtag(index.Name, null, query.ResultsTransformer);

				stale = actions.Staleness.IsIndexStale(index.IndexId, query.Cutoff, query.CutoffEtag);

				if (stale == false && query.Cutoff == null && query.CutoffEtag == null)
				{
					var indexInstance = database.IndexStorage.GetIndexInstance(indexName);
					stale = stale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
				}

				indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index.IndexId);
				var indexFailureInformation = actions.Indexing.GetFailureRate(index.IndexId);
				if (indexFailureInformation.IsInvalidIndex)
				{
					throw new IndexDisabledException(indexFailureInformation);
				}
				docRetriever = new DocumentRetriever(actions, database.ReadTriggers, database.inFlightTransactionalState, query.QueryInputs, idsToLoad);
				var fieldsToFetch = new FieldsToFetch(query.FieldsToFetch, query.IsDistinct,
					viewGenerator.ReduceDefinition == null
						? Constants.DocumentIdFieldName
						: Constants.ReduceKeyFieldName);
				Func<IndexQueryResult, bool> shouldIncludeInResults =
					result => docRetriever.ShouldIncludeResultInQuery(result, index, fieldsToFetch, ShouldSkipDuplicateChecking);
				var indexQueryResults = database.IndexStorage.Query(indexName, query, shouldIncludeInResults, fieldsToFetch, database.IndexQueryTriggers);
				indexQueryResults = new ActiveEnumerable<IndexQueryResult>(indexQueryResults);

				transformerErrors = new List<string>();
				results = database.GetQueryResults(query, viewGenerator, docRetriever,
					from queryResult in indexQueryResults
					let doc = docRetriever.RetrieveDocumentForQuery(queryResult, index, fieldsToFetch, ShouldSkipDuplicateChecking)
					where doc != null
					let _ = nonAuthoritativeInformation |= (doc.NonAuthoritativeInformation ?? false)
					let __ = tryRecordHighlightingAndScoreExplanation(queryResult)
					select doc, transformerErrors);

				Header = new QueryHeaderInformation
				{
					Index = indexName,
					IsStable = stale,
					ResultEtag = resultEtag,
					IndexTimestamp = indexTimestamp.Item1,
					IndexEtag = indexTimestamp.Item2,
					TotalResults = query.TotalSize.Value
				};
			}

			public void Execute(Action<RavenJObject> onResult)
			{
				using (new CurrentTransformationScope(docRetriever))
				{
					foreach (var result in results)
					{
						onResult(result);
					}
					if (transformerErrors.Count > 0)
					{
						throw new InvalidOperationException("The transform results function failed.\r\n" + string.Join("\r\n", transformerErrors));
					}
				}

				Result = new QueryResultWithIncludes
				{
					IndexName = indexName,
					IsStale = stale,
					NonAuthoritativeInformation = nonAuthoritativeInformation,
					SkippedResults = query.SkippedResults.Value,
					TotalResults = query.TotalSize.Value,
					IndexTimestamp = indexTimestamp.Item1,
					IndexEtag = indexTimestamp.Item2,
					ResultEtag = resultEtag,
					IdsToInclude = idsToLoad,
					LastQueryTime = SystemTime.UtcNow,
					Highlightings = highlightings,
					DurationMilliseconds = duration.ElapsedMilliseconds,
					ScoreExplanations = scoreExplanations
				};
			}

			public void Dispose()
			{
				database.RemoveFromCurrentlyRunningQueryList(indexName, queryStat);
			}
		}


		private void RemoveFromCurrentlyRunningQueryList(string index, ExecutingQueryInfo queryStat)
		{
			ConcurrentSet<ExecutingQueryInfo> set;
			if (workContext.CurrentlyRunningQueries.TryGetValue(index, out set) == false)
				return;
			set.TryRemove(queryStat);
		}

		private ExecutingQueryInfo AddToCurrentlyRunningQueryList(string index, IndexQuery query)
		{
			var set = workContext.CurrentlyRunningQueries.GetOrAdd(index, x => new ConcurrentSet<ExecutingQueryInfo>());
			var queryStartTime = DateTime.UtcNow;
			var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query);
			set.Add(executingQueryInfo);
			return executingQueryInfo;
		}

		private IEnumerable<RavenJObject> GetQueryResults(IndexQuery query,
			AbstractViewGenerator viewGenerator,
			DocumentRetriever docRetriever,
			IEnumerable<JsonDocument> results,
			List<string> transformerErrors)
		{
			if (query.PageSize <= 0) // maybe they just want the stats? 
			{
				return Enumerable.Empty<RavenJObject>();
			}

			IndexingFunc transformFunc = null;

			// Check an explicitly declared one first
			if (string.IsNullOrEmpty(query.ResultsTransformer) == false)
			{
				var transformGenerator = IndexDefinitionStorage.GetTransformer(query.ResultsTransformer);

				if (transformGenerator != null && transformGenerator.TransformResultsDefinition != null)
					transformFunc = transformGenerator.TransformResultsDefinition;
				else
					throw new InvalidOperationException("The transformer " + query.ResultsTransformer + " was not found");
			}
			else if (query.SkipTransformResults == false && viewGenerator.TransformResultsDefinition != null)
			{
				transformFunc = source => viewGenerator.TransformResultsDefinition(docRetriever, source);
			}

			if (transformFunc == null)
				return results.Select(x => x.ToJson());

			var dynamicJsonObjects = results.Select(x => new DynamicLuceneOrParentDocumntObject(docRetriever, x.ToJson())).ToArray();
			var robustEnumerator = new RobustEnumerator(workContext.CancellationToken, dynamicJsonObjects.Length)
			{
				OnError =
					(exception, o) =>
					transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o),
														exception.Message))
			};
			return robustEnumerator.RobustEnumeration(
				dynamicJsonObjects.Cast<object>().GetEnumerator(),
				transformFunc)
				.Select(JsonExtensions.ToJObject);
		}


		public IEnumerable<string> QueryDocumentIds(string index, IndexQuery query, out bool stale)
		{
			var queryStat = AddToCurrentlyRunningQueryList(index, query);
			try
			{
				bool isStale = false;
				HashSet<string> loadedIds = null;
				TransactionalStorage.Batch(
					actions =>
					{
						var definition = IndexDefinitionStorage.GetIndexDefinition(index);
						isStale = actions.Staleness.IsIndexStale(definition.IndexId, query.Cutoff, null);

						if (isStale == false && query.Cutoff == null)
						{
							var indexInstance = IndexStorage.GetIndexInstance(index);
							isStale = isStale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
						}

						var indexFailureInformation = actions.Indexing.GetFailureRate(definition.IndexId);

						if (indexFailureInformation.IsInvalidIndex)
						{
							throw new IndexDisabledException(indexFailureInformation);
						}
						loadedIds = new HashSet<string>(from queryResult in IndexStorage.Query(index, query, result => true, new FieldsToFetch(null, false, Constants.DocumentIdFieldName), IndexQueryTriggers)
														select queryResult.Key);
					});
				stale = isStale;
				return loadedIds;
			}
			finally
			{
				RemoveFromCurrentlyRunningQueryList(index, queryStat);
			}
		}

		public void DeleteTransfom(string name)
		{
			IndexDefinitionStorage.RemoveTransformer(name);
		}

		public void DeleteIndex(string name)
		{
			using (IndexDefinitionStorage.TryRemoveIndexContext())
			{
				var instance = IndexDefinitionStorage.GetIndexDefinition(name);
				if (instance == null) return;

				// Set up a flag to signal that this is something we're doing
				TransactionalStorage.Batch(actions => actions.Lists.Set("Raven/Indexes/PendingDeletion", instance.IndexId.ToString(CultureInfo.InvariantCulture), (RavenJObject.FromObject(new
				{
					TimeOfOriginalDeletion = SystemTime.UtcNow,
					instance.IndexId
				})), UuidType.Tasks));

				// Delete the main record synchronously
				IndexDefinitionStorage.RemoveIndex(name);
				IndexStorage.DeleteIndex(instance.IndexId);

				ConcurrentSet<string> _;
				workContext.DoNotTouchAgainIfMissingReferences.TryRemove(instance.IndexId, out _);
				workContext.ClearErrorsFor(name);

				// And delete the data in the background
				StartDeletingIndexData(instance.IndexId);

				// We raise the notification now because as far as we're concerned it is done *now*
				TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => RaiseNotifications(new IndexChangeNotification
				{
					Name = name,
					Type = IndexChangeTypes.IndexRemoved,
				}));
			}
		}

		internal void StartDeletingIndexData(int id)
		{
			//remove the header information in a sync process
			TransactionalStorage.Batch(actions => actions.Indexing.PrepareIndexForDeletion(id));
			var task = Task.Run(() =>
			{
				// Data can take a while
				IndexStorage.DeleteIndexData(id);
				TransactionalStorage.Batch(actions =>
				{
					// And Esent data can take a while too
					actions.Indexing.DeleteIndex(id, WorkContext.CancellationToken);
					if (WorkContext.CancellationToken.IsCancellationRequested)
						return;

					actions.Lists.Remove("Raven/Indexes/PendingDeletion", id.ToString(CultureInfo.InvariantCulture));
				});
			});

			long taskId;
			AddTask(task, null, out taskId);
			PendingTaskAndState value;
			task.ContinueWith(_ => pendingTasks.TryRemove(taskId, out value));
		}

		public Attachment GetStatic(string name)
		{
			if (name == null)
				throw new ArgumentNullException("name");
			name = name.Trim();
			Attachment attachment = null;
			TransactionalStorage.Batch(actions =>
			{
				attachment = actions.Attachments.GetAttachment(name);

				attachment = ProcessAttachmentReadVetoes(name, attachment);

				ExecuteAttachmentReadTriggers(name, attachment);
			});
			return attachment;
		}

		public IEnumerable<AttachmentInformation> GetStaticsStartingWith(string idPrefix, int start, int pageSize)
		{
			if (idPrefix == null) throw new ArgumentNullException("idPrefix");
			IEnumerable<AttachmentInformation> attachments = null;
			TransactionalStorage.Batch(actions =>
			{
				attachments = actions.Attachments.GetAttachmentsStartingWith(idPrefix, start, pageSize)
					.Select(information =>
					{
						var processAttachmentReadVetoes = ProcessAttachmentReadVetoes(information);
						ExecuteAttachmentReadTriggers(processAttachmentReadVetoes);
						return processAttachmentReadVetoes;
					})
					.Where(x => x != null)
					.ToList();
			});
			return attachments;
		}

		private Attachment ProcessAttachmentReadVetoes(string name, Attachment attachment)
		{
			if (attachment == null)
				return null;

			var foundResult = false;
			foreach (var attachmentReadTriggerLazy in AttachmentReadTriggers)
			{
				if (foundResult)
					break;
				var attachmentReadTrigger = attachmentReadTriggerLazy.Value;
				var readVetoResult = attachmentReadTrigger.AllowRead(name, attachment.Data(), attachment.Metadata,
																	 ReadOperation.Load);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						attachment.Data = () => new MemoryStream(new byte[0]);
						attachment.Size = 0;
						attachment.Metadata = new RavenJObject
												{
													{
														"Raven-Read-Veto",
														new RavenJObject
															{
																{"Reason", readVetoResult.Reason},
																{"Trigger", attachmentReadTrigger.ToString()}
															}
														}
												};
						foundResult = true;
						break;
					case ReadVetoResult.ReadAllow.Ignore:
						attachment = null;
						foundResult = true;
						break;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}
			return attachment;
		}

		private void ExecuteAttachmentReadTriggers(string name, Attachment attachment)
		{
			if (attachment == null)
				return;

			foreach (var attachmentReadTrigger in AttachmentReadTriggers)
			{
				attachmentReadTrigger.Value.OnRead(name, attachment);
			}
		}


		private AttachmentInformation ProcessAttachmentReadVetoes(AttachmentInformation attachment)
		{
			if (attachment == null)
				return null;

			var foundResult = false;
			foreach (var attachmentReadTriggerLazy in AttachmentReadTriggers)
			{
				if (foundResult)
					break;
				var attachmentReadTrigger = attachmentReadTriggerLazy.Value;
				var readVetoResult = attachmentReadTrigger.AllowRead(attachment.Key, null, attachment.Metadata,
																	 ReadOperation.Load);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						attachment.Size = 0;
						attachment.Metadata = new RavenJObject
												{
													{
														"Raven-Read-Veto",
														new RavenJObject
															{
																{"Reason", readVetoResult.Reason},
																{"Trigger", attachmentReadTrigger.ToString()}
															}
														}
												};
						foundResult = true;
						break;
					case ReadVetoResult.ReadAllow.Ignore:
						attachment = null;
						foundResult = true;
						break;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}
			return attachment;
		}

		private void ExecuteAttachmentReadTriggers(AttachmentInformation information)
		{
			if (information == null)
				return;

			foreach (var attachmentReadTrigger in AttachmentReadTriggers)
			{
				attachmentReadTrigger.Value.OnRead(information);
			}
		}

		public Etag PutStatic(string name, Etag etag, Stream data, RavenJObject metadata)
		{
			if (name == null)
				throw new ArgumentNullException("name");
			name = name.Trim();

			if (Encoding.Unicode.GetByteCount(name) >= 2048)
				throw new ArgumentException("The key must be a maximum of 2,048 bytes in Unicode, 1,024 characters", "name");

			var locker = putAttachmentSerialLock.GetOrAdd(name, s => new object());
			Monitor.Enter(locker);
			try
			{
				Etag newEtag = Etag.Empty;
				TransactionalStorage.Batch(actions =>
				{
					AssertAttachmentPutOperationNotVetoed(name, metadata, data);

					AttachmentPutTriggers.Apply(trigger => trigger.OnPut(name, data, metadata));

					newEtag = actions.Attachments.AddAttachment(name, etag, data, metadata);

					AttachmentPutTriggers.Apply(trigger => trigger.AfterPut(name, data, metadata, newEtag));

					workContext.ShouldNotifyAboutWork(() => "PUT ATTACHMENT " + name);
				});

				TransactionalStorage
					.ExecuteImmediatelyOrRegisterForSynchronization(() => AttachmentPutTriggers.Apply(trigger => trigger.AfterCommit(name, data, metadata, newEtag)));
				return newEtag;
			}
			finally
			{
				Monitor.Exit(locker);
				putAttachmentSerialLock.TryRemove(name, out locker);
			}
		}

		public void DeleteStatic(string name, Etag etag)
		{
			if (name == null)
				throw new ArgumentNullException("name");
			name = name.Trim();
			TransactionalStorage.Batch(actions =>
			{
				AssertAttachmentDeleteOperationNotVetoed(name);

				AttachmentDeleteTriggers.Apply(x => x.OnDelete(name));

				actions.Attachments.DeleteAttachment(name, etag);

				AttachmentDeleteTriggers.Apply(x => x.AfterDelete(name));

				workContext.ShouldNotifyAboutWork(() => "DELETE ATTACHMENT " + name);
			});

			TransactionalStorage
				.ExecuteImmediatelyOrRegisterForSynchronization(
					() => AttachmentDeleteTriggers.Apply(trigger => trigger.AfterCommit(name)));

		}

		public RavenJArray GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start, int pageSize)
		{
			var list = new RavenJArray();
			GetDocumentsWithIdStartingWith(idPrefix, matches, exclude, start, pageSize, list.Add);
			return list;
		}

		public void GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start, int pageSize, Action<RavenJObject> addDoc)
		{
			if (idPrefix == null)
				throw new ArgumentNullException("idPrefix");
			idPrefix = idPrefix.Trim();
			TransactionalStorage.Batch(actions =>
			{
				bool returnedDocs = false;
				while (true)
				{
					int docCount = 0;
					var documents = actions.Documents.GetDocumentsWithIdStartingWith(idPrefix, start, pageSize);
					var documentRetriever = new DocumentRetriever(actions, ReadTriggers, inFlightTransactionalState);
					foreach (var doc in documents)
					{
						docCount++;
						string keyTest = doc.Key.Substring(idPrefix.Length);
						if (!WildcardMatcher.Matches(matches, keyTest) || WildcardMatcher.MatchesExclusion(exclude, keyTest))
							continue;
						DocumentRetriever.EnsureIdInMetadata(doc);
						var nonAuthoritativeInformationBehavior = inFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);
						JsonDocument document = nonAuthoritativeInformationBehavior != null ? nonAuthoritativeInformationBehavior(doc) : doc;
						document = documentRetriever
							.ExecuteReadTriggers(doc, null, ReadOperation.Load);
						if (document == null)
							continue;

						addDoc(document.ToJson());
						returnedDocs = true;
					}
					if (returnedDocs || docCount == 0)
						break;
					start += docCount;
				}
			});
		}

		public RavenJArray GetDocuments(int start, int pageSize, Etag etag)
		{
			var list = new RavenJArray();
			GetDocuments(start, pageSize, etag, list.Add);
			return list;
		}

		public void GetDocuments(int start, int pageSize, Etag etag, Action<RavenJObject> addDocument)
		{
			TransactionalStorage.Batch(actions =>
			{
				bool returnedDocs = false;
				while (true)
				{
					var documents = etag == null
										? actions.Documents.GetDocumentsByReverseUpdateOrder(start, pageSize)
										: actions.Documents.GetDocumentsAfter(etag, pageSize);
					var documentRetriever = new DocumentRetriever(actions, ReadTriggers, inFlightTransactionalState);
					int docCount = 0;
					foreach (var doc in documents)
					{
						docCount++;
						if (etag != null)
							etag = doc.Etag;
						DocumentRetriever.EnsureIdInMetadata(doc);
						var nonAuthoritativeInformationBehavior = inFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);
						var document = nonAuthoritativeInformationBehavior == null ? doc : nonAuthoritativeInformationBehavior(doc);
						document = documentRetriever
							.ExecuteReadTriggers(document, null, ReadOperation.Load);
						if (document == null)
							continue;

						addDocument(document.ToJson());
						returnedDocs = true;
					}
					if (returnedDocs || docCount == 0)
						break;
					start += docCount;
				}
			});
		}

		public AttachmentInformation[] GetAttachments(int start, int pageSize, Etag etag, string startsWith, long maxSize)
		{
			AttachmentInformation[] attachments = null;

			TransactionalStorage.Batch(actions =>
			{
				if (string.IsNullOrEmpty(startsWith) == false)
					attachments = actions.Attachments.GetAttachmentsStartingWith(startsWith, start, pageSize).ToArray();
				else if (etag != null)
					attachments = actions.Attachments.GetAttachmentsAfter(etag, pageSize, maxSize).ToArray();
				else
					attachments = actions.Attachments.GetAttachmentsByReverseUpdateOrder(start).Take(pageSize).ToArray();

			});
			return attachments;
		}

		public RavenJArray GetIndexNames(int start, int pageSize)
		{
			return new RavenJArray(
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(s => new RavenJValue(s))
				);
		}

		public RavenJArray GetIndexes(int start, int pageSize)
		{
			return new RavenJArray(
				from indexName in IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
				let indexDefinition = IndexDefinitionStorage.GetIndexDefinition(indexName)
				select new RavenJObject
		        {
			        {"name", new RavenJValue(indexName)},
			        {"definition", indexDefinition != null ? RavenJObject.FromObject(indexDefinition) : null},
		        });
		}

		public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag, ScriptedPatchRequest patch,
															   TransactionInformation transactionInformation, bool debugMode = false)
		{
			ScriptedJsonPatcher scriptedJsonPatcher = null;
			var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
				(jsonDoc, size) =>
				{
					scriptedJsonPatcher = new ScriptedJsonPatcher(this);
					return scriptedJsonPatcher.Apply(jsonDoc, patch, size, docId);
				},
				() => null,
				() =>
				{
					if (scriptedJsonPatcher == null)
						return null;
					return scriptedJsonPatcher.CreatedDocs;
				}, debugMode);
			return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
		}

		public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Etag etag,
															   ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata,
															   TransactionInformation transactionInformation, bool debugMode = false)
		{
			ScriptedJsonPatcher scriptedJsonPatcher = null;
			var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
				(jsonDoc, size) =>
				{
					scriptedJsonPatcher = new ScriptedJsonPatcher(this);
					return scriptedJsonPatcher.Apply(jsonDoc, patchExisting, size, docId);
				},
				() =>
				{
					if (patchDefault == null)
						return null;

					scriptedJsonPatcher = new ScriptedJsonPatcher(this);
					var jsonDoc = new RavenJObject();
					jsonDoc[Constants.Metadata] = defaultMetadata ?? new RavenJObject();
					return scriptedJsonPatcher.Apply(new RavenJObject(), patchDefault, 0, docId);
				},
				() =>
				{
					if (scriptedJsonPatcher == null)
						return null;
					return scriptedJsonPatcher.CreatedDocs;
				}, debugMode);
			return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
		}

		public PatchResultData ApplyPatch(string docId, Etag etag, PatchRequest[] patchDoc,
										  TransactionInformation transactionInformation, bool debugMode = false)
		{
			if (docId == null)
				throw new ArgumentNullException("docId");
			return ApplyPatchInternal(docId, etag, transactionInformation,
									  (jsonDoc, size) => new JsonPatcher(jsonDoc).Apply(patchDoc),
									  () => null, () => null, debugMode);
		}

		public PatchResultData ApplyPatch(string docId, Etag etag,
										  PatchRequest[] patchExistingDoc, PatchRequest[] patchDefaultDoc, RavenJObject defaultMetadata,
										  TransactionInformation transactionInformation, bool debugMode = false)
		{
			if (docId == null)
				throw new ArgumentNullException("docId");
			return ApplyPatchInternal(docId, etag, transactionInformation,
									  (jsonDoc, size) => new JsonPatcher(jsonDoc).Apply(patchExistingDoc),
									  () =>
									  {
										  if (patchDefaultDoc == null || patchDefaultDoc.Length == 0)
											  return null;

										  var jsonDoc = new RavenJObject();
										  jsonDoc[Constants.Metadata] = defaultMetadata ?? new RavenJObject();
										  return new JsonPatcher(jsonDoc).Apply(patchDefaultDoc);
									  },
									  () => null, debugMode);
		}

		private PatchResultData ApplyPatchInternal(string docId, Etag etag,
												   TransactionInformation transactionInformation,
												   Func<RavenJObject, int, RavenJObject> patcher,
												   Func<RavenJObject> patcherIfMissing,
												   Func<IList<JsonDocument>> getDocsCreatedInPatch,
												   bool debugMode)
		{
			if (docId == null) throw new ArgumentNullException("docId");
			docId = docId.Trim();
			var result = new PatchResultData
			{
				PatchResult = PatchResult.Patched
			};

			bool shouldRetry = false;
			int[] retries = { 128 };
			Random rand = null;
			do
			{
				TransactionalStorage.Batch(actions =>
				{
					var doc = actions.Documents.DocumentByKey(docId, transactionInformation);
					if (etag != null && doc != null && doc.Etag != etag)
					{
						Debug.Assert(doc.Etag != null);
						throw new ConcurrencyException("Could not patch document '" + docId + "' because non current etag was used")
						{
							ActualETag = doc.Etag,
							ExpectedETag = etag,
						};
					}

					var jsonDoc = (doc != null ? patcher(doc.ToJson(), doc.SerializedSizeOnDisk) : patcherIfMissing());
					if (jsonDoc == null)
					{
						result.PatchResult = PatchResult.DocumentDoesNotExists;
					}
					else
					{
						if (debugMode)
						{
							result.Document = jsonDoc;
							result.PatchResult = PatchResult.Tested;
						}
						else
						{
							try
							{
								Put(doc == null ? docId : doc.Key, (doc == null ? null : doc.Etag), jsonDoc, jsonDoc.Value<RavenJObject>(Constants.Metadata), transactionInformation);

								var docsCreatedInPatch = getDocsCreatedInPatch();
								if (docsCreatedInPatch != null && docsCreatedInPatch.Count > 0)
								{
									foreach (var docFromPatch in docsCreatedInPatch)
									{
										Put(docFromPatch.Key, docFromPatch.Etag, docFromPatch.DataAsJson,
											docFromPatch.Metadata, transactionInformation);
									}
								}
							}
							catch (ConcurrencyException)
							{
								if (actions.IsNested)
									throw;
								if (retries[0]-- > 0)
								{
									shouldRetry = true;
									if (rand == null)
										rand = new Random();
									Thread.Sleep(rand.Next(5, Math.Max(retries[0] * 2, 10)));
									return;
								}
								throw;
							}
							result.PatchResult = PatchResult.Patched;
						}
					}
					if (shouldRetry == false)
						workContext.ShouldNotifyAboutWork(() => "PATCH " + docId);
				});

			} while (shouldRetry);
			return result;
		}

		public BatchResult[] Batch(IList<ICommandData> commands)
		{
			using (TransactionalStorage.WriteLock())
			{
				var shouldRetryIfGotConcurrencyError =
				commands.All(x => (x is PatchCommandData || x is ScriptedPatchCommandData));
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

		public void StartBackup(string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument)
		{
			var document = Get(BackupStatus.RavenBackupStatusDocumentKey, null);
			if (document != null)
			{
				var backupStatus = document.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning)
				{
					throw new InvalidOperationException("Backup is already running");
				}
			}

			bool circularLogging;
			if (incrementalBackup &&
				TransactionalStorage is Raven.Storage.Esent.TransactionalStorage &&
				(bool.TryParse(Configuration.Settings["Raven/Esent/CircularLog"], out circularLogging) == false || circularLogging))
			{
				throw new InvalidOperationException("In order to run incremental backups using Esent you must have circular logging disabled");
			}

			Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
			{
				Started = SystemTime.UtcNow,
				IsRunning = true,
			}), new RavenJObject(), null);
			IndexStorage.FlushMapIndexes();
			IndexStorage.FlushReduceIndexes();
			TransactionalStorage.StartBackupOperation(this, backupDestinationDirectory, incrementalBackup, databaseDocument);
		}

		public static void Restore(RavenConfiguration configuration, string backupLocation, string databaseLocation, Action<string> output, bool defrag)
		{
			using (var transactionalStorage = configuration.CreateTransactionalStorage(() => { }))
			{
				if (!string.IsNullOrWhiteSpace(databaseLocation))
				{
					configuration.DataDirectory = databaseLocation;
				}

				transactionalStorage.Restore(backupLocation, databaseLocation, output, defrag);
			}
		}

		public void ResetIndex(string index)
		{
			var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
			if (indexDefinition == null)
				throw new InvalidOperationException("There is no index named: " + index);
			DeleteIndex(index);
			PutIndex(index, indexDefinition);
		}

		public IndexDefinition GetIndexDefinition(string index)
		{
			return IndexDefinitionStorage.GetIndexDefinition(index);
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
			var fileVersionInfo = FileVersionInfo.GetVersionInfo(typeof(DocumentDatabase).Assembly.Location);
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
				return productVersion ??
					   (productVersion = FileVersionInfo.GetVersionInfo(typeof(DocumentDatabase).Assembly.Location).ProductVersion);
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
		public long GetTransactionalStorageSizeOnDisk()
		{
			return Configuration.RunInMemory ? 0 : TransactionalStorage.GetDatabaseSizeInBytes();
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
			return GetIndexStorageSizeOnDisk() + GetTransactionalStorageSizeOnDisk();
		}

		public Etag GetIndexEtag(string indexName, Etag previousEtag, string resultTransformer = null)
		{
			Etag lastDocEtag = Etag.Empty;
			Etag lastReducedEtag = null;
			bool isStale = false;
			int touchCount = 0;
			TransactionalStorage.Batch(accessor =>
			{
				var indexInstance = IndexStorage.GetIndexInstance(indexName);
				if (indexInstance == null)
					return;
				isStale = (indexInstance.IsMapIndexingInProgress) ||
						  accessor.Staleness.IsIndexStale(indexInstance.indexId, null, null);
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				var indexStats = accessor.Indexing.GetIndexStats(indexInstance.indexId);
				if (indexStats != null)
				{
					lastReducedEtag = indexStats.LastReducedEtag;
				}
				touchCount = accessor.Staleness.GetIndexTouchCount(indexInstance.indexId);
			});


			var indexDefinition = GetIndexDefinition(indexName);
			if (indexDefinition == null)
				return Etag.Empty; // this ensures that we will get the normal reaction of IndexNotFound later on.
			using (var md5 = MD5.Create())
			{
				var list = new List<byte>();
				list.AddRange(indexDefinition.GetIndexHash());
				list.AddRange(Encoding.Unicode.GetBytes(indexName));
				if (string.IsNullOrWhiteSpace(resultTransformer) == false)
				{
					var abstractTransformer = IndexDefinitionStorage.GetTransformer(resultTransformer);
					if (abstractTransformer == null)
						throw new InvalidOperationException("The result transformer: " + resultTransformer + " was not found");
					list.AddRange(abstractTransformer.GetHashCodeBytes());
				}
				list.AddRange(lastDocEtag.ToByteArray());
				list.AddRange(BitConverter.GetBytes(touchCount));
				list.AddRange(BitConverter.GetBytes(isStale));
				if (lastReducedEtag != null)
				{
					list.AddRange(lastReducedEtag.ToByteArray());
				}

				var indexEtag = Etag.Parse(md5.ComputeHash(list.ToArray()));

				if (previousEtag != null && previousEtag != indexEtag)
				{
					// the index changed between the time when we got it and the time 
					// we actually call this, we need to return something random so that
					// the next time we won't get 304

					return Etag.InvalidEtag;
				}

				return indexEtag;
			}
		}

		public int BulkInsert(BulkInsertOptions options, IEnumerable<IEnumerable<JsonDocument>> docBatches, Guid operationId)
		{
			var documents = 0;
			TransactionalStorage.Batch(accessor =>
			{
				RaiseNotifications(new BulkInsertChangeNotification
				{
					OperationId = operationId,
					Type = DocumentChangeTypes.BulkInsertStarted
				});
				foreach (var docs in docBatches)
				{
					WorkContext.CancellationToken.ThrowIfCancellationRequested();

					var inserts = 0;
					var batch = 0;
					var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

					var docsToInsert = docs.ToArray();

					foreach (var doc in docsToInsert)
					{
						try
						{
							RemoveReservedProperties(doc.DataAsJson);
							RemoveMetadataReservedProperties(doc.Metadata);

							if (options.CheckReferencesInIndexes)
								keys.Add(doc.Key);
							documents++;
							batch++;
							AssertPutOperationNotVetoed(doc.Key, doc.Metadata, doc.DataAsJson, null);
							foreach (var trigger in PutTriggers)
							{
								trigger.Value.OnPut(doc.Key, doc.DataAsJson, doc.Metadata, null);
							}
							var result = accessor.Documents.InsertDocument(doc.Key, doc.DataAsJson, doc.Metadata, options.CheckForUpdates);
							if (result.Updated == false)
								inserts++;

							doc.Etag = result.Etag;

							doc.Metadata.EnsureSnapshot(
							"Metadata was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");
							doc.DataAsJson.EnsureSnapshot(
							"Document was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");


							foreach (var trigger in PutTriggers)
							{
								trigger.Value.AfterPut(doc.Key, doc.DataAsJson, doc.Metadata, result.Etag, null);
							}
						}
						catch (Exception e)
						{
							RaiseNotifications(new BulkInsertChangeNotification
							{
								OperationId = operationId,
								Message = e.Message,
								Etag = doc.Etag,
								Id = doc.Key,
								Type = DocumentChangeTypes.BulkInsertError
							});

							throw;
						}
					}
					if (options.CheckReferencesInIndexes)
					{
						foreach (var key in keys)
						{
							CheckReferenceBecauseOfDocumentUpdate(key, accessor);
						}
					}
					accessor.Documents.IncrementDocumentCount(inserts);
					accessor.General.PulseTransaction();
					etagSynchronizer.UpdateSynchronizationState(docsToInsert);

					workContext.ShouldNotifyAboutWork(() => "BulkInsert batch of " + batch + " docs");
					workContext.NotifyAboutWork(); // forcing notification so we would start indexing right away
				}

				RaiseNotifications(new BulkInsertChangeNotification
				{
					OperationId = operationId,
					Type = DocumentChangeTypes.BulkInsertEnded
				});
				if (documents == 0)
					return;
				workContext.ShouldNotifyAboutWork(() => "BulkInsert of " + documents + " docs");
			});
			return documents;
		}

		public TouchedDocumentInfo GetRecentTouchesFor(string key)
		{
			TouchedDocumentInfo info;
			recentTouches.TryGetValue(key, out info);
			return info;
		}

		public void AddTask(Task task, object state, out long id)
		{
			if (task.Status == TaskStatus.Created)
				throw new ArgumentException("Task must be started before it gets added to the database.", "task");
			var localId = id = Interlocked.Increment(ref pendingTaskCounter);
			pendingTasks.TryAdd(localId, new PendingTaskAndState
			{
				Task = task,
				State = state
			});
		}

		public object GetTaskState(long id)
		{
			PendingTaskAndState value;
			if (pendingTasks.TryGetValue(id, out value))
			{
				if (value.Task.IsFaulted || value.Task.IsCanceled)
					value.Task.Wait(); //throws
				return value.State;
			}
			return null;
		}

		public RavenJArray GetTransformerNames(int start, int pageSize)
		{
			return new RavenJArray(
			IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
				.Select(s => new RavenJValue(s))
			);
		}

		public RavenJArray GetTransformers(int start, int pageSize)
		{
			return new RavenJArray(
			IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
				.Select(
					indexName => new RavenJObject
							{
								{"name", new RavenJValue(indexName) },
								{"definition", RavenJObject.FromObject(IndexDefinitionStorage.GetTransformerDefinition(indexName))}
							}));

		}

		public JsonDocument GetWithTransformer(string key, string transformer, TransactionInformation transactionInformation, Dictionary<string, RavenJToken> queryInputs)
		{
			JsonDocument result = null;
			TransactionalStorage.Batch(
			actions =>
			{
				var docRetriever = new DocumentRetriever(actions, ReadTriggers, inFlightTransactionalState, queryInputs);
				using (new CurrentTransformationScope(docRetriever))
				{
					var document = Get(key, transactionInformation);
					if (document == null)
						return;

					var storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
					if (storedTransformer == null)
						throw new InvalidOperationException("No transformer with the name: " + transformer);

					var transformed = storedTransformer.TransformResultsDefinition(new[] { new DynamicJsonObject(document.ToJson()) })
									 .Select(x => JsonExtensions.ToJObject(x))
									 .ToArray();

					if (transformed.Length == 0)
						return;

					result = new JsonDocument
					{
						Etag = document.Etag.HashWith(storedTransformer.GetHashCodeBytes()).HashWith(docRetriever.Etag),
						NonAuthoritativeInformation = document.NonAuthoritativeInformation,
						LastModified = document.LastModified,
						DataAsJson = new RavenJObject { { "$values", new RavenJArray(transformed) } },
					};
				}
			});
			return result;
		}

		public TransformerDefinition GetTransformerDefinition(string name)
		{
			return IndexDefinitionStorage.GetTransformerDefinition(name);
		}
	}
}
