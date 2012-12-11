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
using System.Transactions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Commercial;
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
using Raven.Database.Exceptions;
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
using Index = Raven.Database.Indexing.Index;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Database
{
	public class DocumentDatabase : IUuidGenerator, IDisposable
	{
		[ImportMany]
		public OrderedPartCollection<AbstractRequestResponder> RequestResponders { get; set; }

		[ImportMany]
		public OrderedPartCollection<IStartupTask> StartupTasks { get; set; }

		[ImportMany]
		public OrderedPartCollection<AbstractAttachmentPutTrigger> AttachmentPutTriggers { get; set; }

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

		private readonly List<IDisposable> toDispose = new List<IDisposable>();

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

		private readonly ConcurrentDictionary<Guid, CommittableTransaction> promotedTransactions = new ConcurrentDictionary<Guid, CommittableTransaction>();

		/// <summary>
		/// This is required to ensure serial generation of etags during puts
		/// </summary>
		private readonly object putSerialLock = new object();

		/// <summary>
		/// Requires to avoid having serialize writes to the same attachments
		/// </summary>
		private readonly ConcurrentDictionary<string, object> putAttachmentSerialLock = new ConcurrentDictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

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
		private long currentEtagBase;

		public DocumentDatabase(InMemoryRavenConfiguration configuration)
		{
			if (configuration.IsTenantDatabase == false)
			{
				validateLicense = new ValidateLicense();
				validateLicense.Execute(configuration);
			}
			AppDomain.CurrentDomain.DomainUnload += DomainUnloadOrProcessExit;
			AppDomain.CurrentDomain.ProcessExit += DomainUnloadOrProcessExit;

			Name = configuration.DatabaseName;
			if (configuration.CustomTaskScheduler != null)
			{
				backgroundTaskScheduler = configuration.CustomTaskScheduler;
			}
			else
			{
				backgroundTaskScheduler = TaskScheduler.Current;
			}

			ExtensionsState = new AtomicDictionary<object>();
			Configuration = configuration;

			ExecuteAlterConfiguration();

			configuration.Container.SatisfyImportsOnce(this);

			workContext = new WorkContext
			{
				DatabaseName = Name,
				IndexUpdateTriggers = IndexUpdateTriggers,
				ReadTriggers = ReadTriggers,
				RaiseIndexChangeNotification = RaiseNotifications,
				TaskScheduler = backgroundTaskScheduler,
				Configuration = configuration
			};

			TransactionalStorage = configuration.CreateTransactionalStorage(workContext.HandleWorkNotifications);

			try
			{
				TransactionalStorage.Initialize(this, DocumentCodecs);
			}
			catch (Exception)
			{
				TransactionalStorage.Dispose();
				throw;
			}

			try
			{

				TransactionalStorage.Batch(actions => currentEtagBase = actions.General.GetNextIdentityValue("Raven/Etag"));

				TransportState = new TransportState();

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

				indexingExecuter = new IndexingExecuter(workContext);

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
			workContext.TransactionaStorage = TransactionalStorage;
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
			using (new DisposableAction(() => LogContext.DatabaseName.Value = null))
			{
				LogContext.DatabaseName.Value = Name;
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
					Prefetches = workContext.FutureBatchStats.OrderBy(x => x.Timestamp).ToArray(),
					CountOfIndexes = IndexStorage.Indexes.Length,
					DatabaseCacheSizeInBytes = workContext.TransactionaStorage.GetDatabaseCacheSizeInBytes(),
					DatabaseCacheSizeInMB = workContext.TransactionaStorage.GetDatabaseCacheSizeInBytes() / 1024.0m / 1024.0m,
					Errors = workContext.Errors,
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
						)
				};

				TransactionalStorage.Batch(actions =>
				{
					result.LastDocEtag = actions.Staleness.GetMostRecentDocumentEtag();
					result.LastAttachmentEtag = actions.Staleness.GetMostRecentAttachmentEtag();

					result.ApproximateTaskCount = actions.Tasks.ApproximateTaskCount;
					result.CountOfDocuments = actions.Documents.GetDocumentsCount();
					result.StaleIndexes = IndexStorage.Indexes
						.Where(s => actions.Staleness.IsIndexStale(s, null, null)).ToArray();
					result.Indexes = actions.Indexing.GetIndexesStats().ToArray();
				});

				foreach (var index in result.Indexes)
				{
					index.LastQueryTimestamp = IndexStorage.GetLastQueryTime(index.Name);
					index.Performance = IndexStorage.GetIndexingPerformance(index.Name);
				}

				return result;
			}
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
			indexingBackgroundTask.Wait();
			reducingBackgroundTask.Wait();

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
			indexingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
				indexingExecuter.Execute,
				CancellationToken.None, TaskCreationOptions.LongRunning, backgroundTaskScheduler);
			reducingBackgroundTask = System.Threading.Tasks.Task.Factory.StartNew(
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

		public void RaiseNotifications(DocumentChangeNotification obj)
		{
			TransportState.Send(obj);
		}

		public void RaiseNotifications(IndexChangeNotification obj)
		{
			TransportState.Send(obj);
		}

		public void RunIdleOperations()
		{
			var tryEnter = Monitor.TryEnter(idleLocker);
			try
			{
				if (tryEnter == false)
					return;
				TransportState.OnIdle();
				IndexStorage.RunIdleOperations();
			}
			finally
			{
				if (tryEnter)
					Monitor.Exit(idleLocker);
			}
		}

		private long sequentialUuidCounter;

		public Guid CreateSequentialUuid()
		{
			var ticksAsBytes = BitConverter.GetBytes(currentEtagBase);
			Array.Reverse(ticksAsBytes);
			var increment = Interlocked.Increment(ref sequentialUuidCounter);
			var currentAsBytes = BitConverter.GetBytes(increment);
			Array.Reverse(currentAsBytes);
			var bytes = new byte[16];
			Array.Copy(ticksAsBytes, 0, bytes, 0, ticksAsBytes.Length);
			Array.Copy(currentAsBytes, 0, bytes, 8, currentAsBytes.Length);
			return bytes.TransfromToGuidWithProperSorting();
		}


		public JsonDocument Get(string key, TransactionInformation transactionInformation)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();
			JsonDocument document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.Documents.DocumentByKey(key, transactionInformation);
			});

			DocumentRetriever.EnsureIdInMetadata(document);
			return new DocumentRetriever(null, ReadTriggers)
				.ExecuteReadTriggers(document, transactionInformation, ReadOperation.Load);
		}

		public JsonDocumentMetadata GetDocumentMetadata(string key, TransactionInformation transactionInformation)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();
			JsonDocumentMetadata document = null;
			TransactionalStorage.Batch(actions =>
			{
				document = actions.Documents.DocumentMetadataByKey(key, transactionInformation);
			});

			DocumentRetriever.EnsureIdInMetadata(document);
			return new DocumentRetriever(null, ReadTriggers)
				.ProcessReadVetoes(document, transactionInformation, ReadOperation.Load);
		}


		public void PutDocumentMetadata(string key, RavenJObject metadata)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();
			TransactionalStorage.Batch(actions => actions.Documents.PutDocumentMetadata(key, metadata));
		}

		public PutResult Put(string key, Guid? etag, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			workContext.DocsPerSecIncreaseBy(1);
			key = string.IsNullOrWhiteSpace(key) ? Guid.NewGuid().ToString() : key.Trim();
			RemoveReservedProperties(document);
			RemoveReservedProperties(metadata);
			Guid newEtag = Guid.Empty;
			lock (putSerialLock)
			{
				TransactionalStorage.Batch(actions =>
				{
					if (key.EndsWith("/"))
					{
						key += GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions, transactionInformation);
					}
					AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
					if (transactionInformation == null)
					{
						PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, null));

						var addDocumentResult = actions.Documents.AddDocument(key, etag, document, metadata);
						newEtag = addDocumentResult.Etag;

						metadata.EnsureSnapshot("Metadata was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");
						document.EnsureSnapshot("Document was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");

						PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, newEtag, null));

						actions.AfterCommit(new JsonDocument
						{
							Metadata = metadata,
							Key = key,
							DataAsJson = document,
							Etag = newEtag,
							LastModified = addDocumentResult.SavedAt,
							SkipDeleteFromIndex = addDocumentResult.Updated == false
						}, indexingExecuter.AfterCommit);
						TransactionalStorage
							.ExecuteImmediatelyOrRegisterForSyncronization(() =>
							{
								PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata, newEtag));
								RaiseNotifications(new DocumentChangeNotification
								{
									Id = key,
									Type = DocumentChangeTypes.Put,
									Etag = newEtag,
								});
							});
					}
					else
					{
						newEtag = actions.Transactions.AddDocumentInTransaction(key, etag,
																				document, metadata, transactionInformation);
					}
					workContext.ShouldNotifyAboutWork(() => "PUT " + key);
				});
			}

			log.Debug("Put document {0} with etag {1}", key, newEtag);
			return new PutResult
			{
				Key = key,
				ETag = newEtag
			};
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

		private static void RemoveReservedProperties(RavenJObject document)
		{
			document.Remove(string.Empty);
			var toRemove = document.Keys.Where(propertyName => propertyName.StartsWith("@")).ToList();
			foreach (var propertyName in toRemove)
			{
				document.Remove(propertyName);
			}
		}

		public bool Delete(string key, Guid? etag, TransactionInformation transactionInformation)
		{
			RavenJObject metadata;
			return Delete(key, etag, transactionInformation, out metadata);
		}

		public bool Delete(string key, Guid? etag, TransactionInformation transactionInformation, out RavenJObject metadata)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			key = key.Trim();

			lock (putSerialLock)
			{
				var deleted = false;
				log.Debug("Delete a document with key: {0} and etag {1}", key, etag);
				RavenJObject metadataVar = null;
				TransactionalStorage.Batch(actions =>
				{
					AssertDeleteOperationNotVetoed(key, transactionInformation);
					if (transactionInformation == null)
					{
						DeleteTriggers.Apply(trigger => trigger.OnDelete(key, null));

						Guid? deletedETag;
						if (actions.Documents.DeleteDocument(key, etag, out metadataVar, out deletedETag))
						{
							deleted = true;
							WorkContext.MarkDeleted(key);
							foreach (var indexName in IndexDefinitionStorage.IndexNames)
							{
								AbstractViewGenerator abstractViewGenerator = IndexDefinitionStorage.GetViewGenerator(indexName);
								if (abstractViewGenerator == null)
									continue;

								var token = metadataVar.Value<string>(Constants.RavenEntityName);

								if (token != null && // the document has a entity name
									abstractViewGenerator.ForEntityNames.Count > 0) // the index operations on specific entities
								{
									if (abstractViewGenerator.ForEntityNames.Contains(token) == false)
										continue;
								}

								string indexNameCopy = indexName;
								var task = actions.GetTask(x => x.Index == indexNameCopy, new RemoveFromIndexTask
								{
									Index = indexNameCopy
								});
								task.Keys.Add(key);
							}
							indexingExecuter.AfterDelete(key, deletedETag);
							DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, null));
						}

						TransactionalStorage
							.ExecuteImmediatelyOrRegisterForSyncronization(() =>
							{
								DeleteTriggers.Apply(trigger => trigger.AfterCommit(key));
								RaiseNotifications(new DocumentChangeNotification
								{
									Id = key,
									Type = DocumentChangeTypes.Delete,
								});
							});

					}
					else
					{
						deleted = actions.Transactions.DeleteDocumentInTransaction(transactionInformation, key, etag);
					}
					workContext.ShouldNotifyAboutWork(() => "DEL " + key);
				});

				metadata = metadataVar;
				return deleted;
			}
		}

		public bool HasTransaction(Guid txId)
		{
			bool exists = false;
			TransactionalStorage.Batch(accessor =>
			{
				exists = accessor.Transactions.TransactionExists(txId);
			});
			return exists;
		}

		public void Commit(Guid txId)
		{
			try
			{
				lock (putSerialLock)
				{
					TransactionalStorage.Batch(actions =>
					{
						actions.Transactions.CompleteTransaction(txId, doc =>
						{
							// doc.Etag - represent the _modified_ document etag, and we already
							// checked etags on previous PUT/DELETE, so we don't pass it here
							if (doc.Delete)
								Delete(doc.Key, null, null);
							else
								Put(doc.Key, null,
									doc.Data,
									doc.Metadata, null);
						});
						actions.Attachments.DeleteAttachment("transactions/recoveryInformation/" + txId, null);
						workContext.ShouldNotifyAboutWork(() => "COMMIT " + txId);
					});
				}
				TryCompletePromotedTransaction(txId);
			}
			catch (Exception e)
			{
				if (TransactionalStorage.HandleException(e))
					return;
				throw;
			}
		}

		private void TryCompletePromotedTransaction(Guid txId)
		{
			CommittableTransaction transaction;
			if (!promotedTransactions.TryRemove(txId, out transaction))
				return;
			System.Threading.Tasks.Task.Factory.FromAsync(transaction.BeginCommit, transaction.EndCommit, null)
				.ContinueWith(task =>
				{
					if (task.Exception != null)
						log.WarnException("Could not commit dtc transaction", task.Exception);
					try
					{
						transaction.Dispose();
					}
					catch (Exception e)
					{
						log.WarnException("Could not dispose of dtc transaction", e);
					}
				});
		}

		private void TryUndoPromotedTransaction(Guid txId)
		{
			CommittableTransaction transaction;
			if (!promotedTransactions.TryRemove(txId, out transaction))
				return;
			transaction.Rollback();
			transaction.Dispose();
		}

		public void Rollback(Guid txId)
		{
			try
			{
				TransactionalStorage.Batch(actions =>
				{
					actions.Transactions.RollbackTransaction(txId);
					actions.Attachments.DeleteAttachment("transactions/recoveryInformation/" + txId, null);
					workContext.ShouldNotifyAboutWork(() => "ROLLBACK " + txId);
				});
				TryUndoPromotedTransaction(txId);
			}
			catch (Exception e)
			{
				if (TransactionalStorage.HandleException(e))
					return;

				throw;
			}
		}

		// only one index can be created at any given time
		// the method already handle attempts to create the same index, so we don't have to 
		// worry about this.
		[MethodImpl(MethodImplOptions.Synchronized)]
		public string PutIndex(string name, IndexDefinition definition)
		{
			if (name == null)
				throw new ArgumentNullException("name");

			name = name.Trim();

			switch (FindIndexCreationOptions(definition, ref name))
			{
				case IndexCreationOptions.Noop:
					return name;
				case IndexCreationOptions.Update:
					// ensure that the code can compile
					new DynamicViewCompiler(name, definition, Extensions, IndexDefinitionStorage.IndexDefinitionsPath, Configuration).GenerateInstance();
					DeleteIndex(name);
					break;
			}

			// this has to happen in this fashion so we will expose the in memory status after the commit, but 
			// before the rest of the world is notified about this.
			IndexDefinitionStorage.CreateAndPersistIndex(definition);
			IndexStorage.CreateIndexImplementation(definition);

			TransactionalStorage.Batch(actions =>
			{
				actions.Indexing.AddIndex(name, definition.IsMapReduce);
				workContext.ShouldNotifyAboutWork(() => "PUT INDEX " + name);

			});

			// The act of adding it here make it visible to other threads
			// we have to do it in this way so first we prepare all the elements of the 
			// index, then we add it to the storage in a way that make it public
			IndexDefinitionStorage.AddIndex(name, definition);

			workContext.ClearErrorsFor(name);

			TransactionalStorage.ExecuteImmediatelyOrRegisterForSyncronization(() => RaiseNotifications(new IndexChangeNotification
			{
				Name = name,
				Type = IndexChangeTypes.IndexAdded,
			}));

			return name;
		}

		private IndexCreationOptions FindIndexCreationOptions(IndexDefinition definition, ref string name)
		{
			definition.Name = name = IndexDefinitionStorage.FixupIndexName(name);
			definition.RemoveDefaultValues();
			IndexDefinitionStorage.ResolveAnalyzers(definition);
			var findIndexCreationOptions = IndexDefinitionStorage.FindIndexCreationOptions(definition);
			return findIndexCreationOptions;
		}

		public QueryResultWithIncludes Query(string index, IndexQuery query)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			var list = new List<RavenJObject>();
			var stale = false;
			Tuple<DateTime, Guid> indexTimestamp = Tuple.Create(DateTime.MinValue, Guid.Empty);
			Guid resultEtag = Guid.Empty;
			var nonAuthoritativeInformation = false;
			var idsToLoad = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			TransactionalStorage.Batch(
				actions =>
				{
					var viewGenerator = IndexDefinitionStorage.GetViewGenerator(index);
					if (viewGenerator == null)
						throw new IndexDoesNotExistsException("Could not find index named: " + index);

					resultEtag = GetIndexEtag(index, null);

					stale = actions.Staleness.IsIndexStale(index, query.Cutoff, query.CutoffEtag);

					indexTimestamp = actions.Staleness.IndexLastUpdatedAt(index);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index);
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					var docRetriever = new DocumentRetriever(actions, ReadTriggers, idsToLoad);
					var indexDefinition = GetIndexDefinition(index);
					var fieldsToFetch = new FieldsToFetch(query.FieldsToFetch, query.AggregationOperation,
														  viewGenerator.ReduceDefinition == null
															? Constants.DocumentIdFieldName
															: Constants.ReduceKeyFieldName);
					Func<IndexQueryResult, bool> shouldIncludeInResults =
						result => docRetriever.ShouldIncludeResultInQuery(result, indexDefinition, fieldsToFetch);
					var collection = from queryResult in IndexStorage.Query(index, query, shouldIncludeInResults, fieldsToFetch, IndexQueryTriggers)
									 select docRetriever.RetrieveDocumentForQuery(queryResult, indexDefinition, fieldsToFetch)
										 into doc
										 where doc != null
										 let _ = nonAuthoritativeInformation |= (doc.NonAuthoritativeInformation ?? false)
										 select doc;

					var transformerErrors = new List<string>();
					IEnumerable<RavenJObject> results;
					if (query.SkipTransformResults == false &&
						query.PageSize > 0 && // maybe they just want the stats?
						viewGenerator.TransformResultsDefinition != null)
					{
						var dynamicJsonObjects = collection.Select(x => new DynamicJsonObject(x.ToJson())).ToArray();
						var robustEnumerator = new RobustEnumerator(workContext, dynamicJsonObjects.Length)
						{
							OnError =
								(exception, o) =>
								transformerErrors.Add(string.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o),
														 exception.Message))
						};
						results =
							robustEnumerator.RobustEnumeration(
								dynamicJsonObjects.Cast<object>().GetEnumerator(),
								source => viewGenerator.TransformResultsDefinition(docRetriever, source))
								.Select(JsonExtensions.ToJObject);
					}
					else
					{
						results = collection.Select(x => x.ToJson());
					}

					list.AddRange(results);

					if (transformerErrors.Count > 0)
					{
						throw new InvalidOperationException("The transform results function failed.\r\n" + string.Join("\r\n", transformerErrors));
					}
				});
			return new QueryResultWithIncludes
			{
				IndexName = index,
				Results = list,
				IsStale = stale,
				NonAuthoritativeInformation = nonAuthoritativeInformation,
				SkippedResults = query.SkippedResults.Value,
				TotalResults = query.TotalSize.Value,
				IndexTimestamp = indexTimestamp.Item1,
				IndexEtag = indexTimestamp.Item2,
				ResultEtag = resultEtag,
				IdsToInclude = idsToLoad,
				LastQueryTime = SystemTime.UtcNow
			};
		}

		public IEnumerable<string> QueryDocumentIds(string index, IndexQuery query, out bool stale)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			bool isStale = false;
			HashSet<string> loadedIds = null;
			TransactionalStorage.Batch(
				actions =>
				{
					isStale = actions.Staleness.IsIndexStale(index, query.Cutoff, null);
					var indexFailureInformation = actions.Indexing.GetFailureRate(index)
;
					if (indexFailureInformation.IsInvalidIndex)
					{
						throw new IndexDisabledException(indexFailureInformation);
					}
					loadedIds = new HashSet<string>(from queryResult in IndexStorage.Query(index, query, result => true, new FieldsToFetch(null, AggregationOperation.None, Constants.DocumentIdFieldName), IndexQueryTriggers)
													select queryResult.Key);
				});
			stale = isStale;
			return loadedIds;
		}

		public void DeleteIndex(string name)
		{
			using (IndexDefinitionStorage.TryRemoveIndexContext())
			{
				name = IndexDefinitionStorage.FixupIndexName(name);
				IndexDefinitionStorage.RemoveIndex(name);
				IndexStorage.DeleteIndex(name);
				//we may run into a conflict when trying to delete if the index is currently
				//busy indexing documents, worst case scenario, we will have an orphaned index
				//row which will get cleaned up on next db restart.
				for (var i = 0; i < 10; i++)
				{
					try
					{
						TransactionalStorage.Batch(action =>
						{
							action.Indexing.DeleteIndex(name);

							workContext.ShouldNotifyAboutWork(() => "DELETE INDEX " + name);
						});

						TransactionalStorage.ExecuteImmediatelyOrRegisterForSyncronization(() => RaiseNotifications(new IndexChangeNotification
						{
							Name = name,
							Type = IndexChangeTypes.IndexRemoved,
						}));

						return;
					}
					catch (ConcurrencyException)
					{
						Thread.Sleep(100);
					}
				}
			}
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

		public Guid PutStatic(string name, Guid? etag, Stream data, RavenJObject metadata)
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
				Guid newEtag = Guid.Empty;
				TransactionalStorage.Batch(actions =>
				{
					AssertAttachmentPutOperationNotVetoed(name, metadata, data);

					AttachmentPutTriggers.Apply(trigger => trigger.OnPut(name, data, metadata));

					newEtag = actions.Attachments.AddAttachment(name, etag, data, metadata);

					AttachmentPutTriggers.Apply(trigger => trigger.AfterPut(name, data, metadata, newEtag));

					workContext.ShouldNotifyAboutWork(() => "PUT ATTACHMENT " + name);
				});

				TransactionalStorage
					.ExecuteImmediatelyOrRegisterForSyncronization(() => AttachmentPutTriggers.Apply(trigger => trigger.AfterCommit(name, data, metadata, newEtag)));
				return newEtag;
			}
			finally
			{
				Monitor.Exit(locker);
				putAttachmentSerialLock.TryRemove(name, out locker);
			}
		}

		public void DeleteStatic(string name, Guid? etag)
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
				.ExecuteImmediatelyOrRegisterForSyncronization(
					() => AttachmentDeleteTriggers.Apply(trigger => trigger.AfterCommit(name)));

		}

		public RavenJArray GetDocumentsWithIdStartingWith(string idPrefix, string matches, int start, int pageSize)
		{
			if (idPrefix == null)
				throw new ArgumentNullException("idPrefix");
			idPrefix = idPrefix.Trim();
			var list = new RavenJArray();
			TransactionalStorage.Batch(actions =>
			{
				var documents = actions.Documents.GetDocumentsWithIdStartingWith(idPrefix, start, pageSize);
				var documentRetriever = new DocumentRetriever(actions, ReadTriggers);
				foreach (var doc in documents)
				{
					if (WildcardMatcher.Matches(matches, doc.Key.Substring(idPrefix.Length)) == false)
						continue;
					DocumentRetriever.EnsureIdInMetadata(doc);
					var document = documentRetriever
						.ExecuteReadTriggers(doc, null, ReadOperation.Load);
					if (document == null)
						continue;

					list.Add(document.ToJson());
				}
			});
			return list;
		}

		public RavenJArray GetDocuments(int start, int pageSize, Guid? etag)
		{
			var list = new RavenJArray();
			TransactionalStorage.Batch(actions =>
			{
				var documents = etag == null ?
					actions.Documents.GetDocumentsByReverseUpdateOrder(start, pageSize) :
					actions.Documents.GetDocumentsAfter(etag.Value, pageSize);
				var documentRetriever = new DocumentRetriever(actions, ReadTriggers);
				foreach (var doc in documents)
				{
					DocumentRetriever.EnsureIdInMetadata(doc);
					var document = documentRetriever
						.ExecuteReadTriggers(doc, null, ReadOperation.Load);
					if (document == null)
						continue;

					list.Add(document.ToJson());
				}
			});
			return list;
		}

		public AttachmentInformation[] GetAttachments(int start, int pageSize, Guid? etag, string startsWith, long maxSize)
		{
			AttachmentInformation[] attachments = null;

			TransactionalStorage.Batch(actions =>
			{
				if (string.IsNullOrEmpty(startsWith) == false)
					attachments = actions.Attachments.GetAttachmentsStartingWith(startsWith, start, pageSize).ToArray();
				else if (etag != null)
					attachments = actions.Attachments.GetAttachmentsAfter(etag.Value, pageSize, maxSize).ToArray();
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
				IndexDefinitionStorage.IndexNames.Skip(start).Take(pageSize)
					.Select(
						indexName => new RavenJObject
							{
								{"name", new RavenJValue(indexName) },
								{"definition", RavenJObject.FromObject(IndexDefinitionStorage.GetIndexDefinition(indexName))}
							}));
		}

		public Tuple<PatchResultData, List<string>> ApplyPatch(string docId, Guid? etag, ScriptedPatchRequest patch, TransactionInformation transactionInformation, bool debugMode = false)
		{
			ScriptedJsonPatcher scriptedJsonPatcher = null;
			var applyPatchInternal = ApplyPatchInternal(docId, etag, transactionInformation,
				jsonDoc =>
				{
					scriptedJsonPatcher = new ScriptedJsonPatcher(
						loadDocument: id =>
										{
											var jsonDocument = Get(id, transactionInformation);
											return jsonDocument == null ? null : jsonDocument.ToJson();
										});
					return scriptedJsonPatcher.Apply(jsonDoc, patch);
				}, debugMode);
			return Tuple.Create(applyPatchInternal, scriptedJsonPatcher == null ? new List<string>() : scriptedJsonPatcher.Debug);
		}

		public PatchResultData ApplyPatch(string docId, Guid? etag, PatchRequest[] patchDoc, TransactionInformation transactionInformation, bool debugMode = false)
		{

			if (docId == null)
				throw new ArgumentNullException("docId");
			return ApplyPatchInternal(docId, etag, transactionInformation, jsonDoc => new JsonPatcher(jsonDoc).Apply(patchDoc), debugMode);
		}

		private PatchResultData ApplyPatchInternal(string docId, Guid? etag,
												TransactionInformation transactionInformation,
												Func<RavenJObject, RavenJObject> patcher, bool debugMode)
		{
			if (docId == null) throw new ArgumentNullException("docId");
			docId = docId.Trim();
			var result = new PatchResultData
			{
				PatchResult = PatchResult.Patched
			};

			bool shouldRetry = false;
			int[] retries = { 128 };
			do
			{
				TransactionalStorage.Batch(actions =>
				{
					var doc = actions.Documents.DocumentByKey(docId, transactionInformation);
					if (doc == null)
					{
						result.PatchResult = PatchResult.DocumentDoesNotExists;
					}
					else if (etag != null && doc.Etag != etag.Value)
					{
						Debug.Assert(doc.Etag != null);
						throw new ConcurrencyException("Could not patch document '" + docId + "' because non current etag was used")
						{
							ActualETag = doc.Etag.Value,
							ExpectedETag = etag.Value,
						};
					}
					else
					{
						var jsonDoc = patcher(doc.ToJson());
						if (debugMode)
						{
							result.Document = jsonDoc;
							result.PatchResult = PatchResult.Tested;
						}
						else
						{
							try
							{
								Put(doc.Key, doc.Etag, jsonDoc, jsonDoc.Value<RavenJObject>("@metadata"), transactionInformation);
							}
							catch (ConcurrencyException)
							{
								if (retries[0]-- > 0)
								{
									shouldRetry = true;
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

		public BatchResult[] Batch(IEnumerable<ICommandData> commands)
		{
			var results = new List<BatchResult>();

			var commandDatas = commands.ToArray();
			int retries = 128;
			var shouldLock = commandDatas.Any(x => (x is PutCommandData || x is PatchCommandData || x is ScriptedPatchCommandData || x is DeleteCommandData));
			var shouldRetryIfGotConcurrencyError = commandDatas.All(x => (x is PatchCommandData || x is ScriptedPatchCommandData));
			bool shouldRetry = false;
			if (shouldLock)
				Monitor.Enter(putSerialLock);
			try
			{
				var sp = Stopwatch.StartNew();
				do
				{
					try
					{
						TransactionalStorage.Batch(actions =>
						{
							foreach (var command in commandDatas)
							{
								command.Execute(this);
								results.Add(new BatchResult
								{
									Method = command.Method,
									Key = command.Key,
									Etag = command.Etag,
									Metadata = command.Metadata,
									AdditionalData = command.AdditionalData
								});
							}
						});
					}
					catch (ConcurrencyException)
					{
						if (shouldRetryIfGotConcurrencyError && retries-- > 128)
						{
							shouldRetry = true;
							results.Clear();
							continue;
						}
						throw;
					}
				} while (shouldRetry);
				log.Debug("Successfully executed {0} commands in {1}", results.Count, sp.Elapsed);
			}
			finally
			{
				if (shouldLock)
					Monitor.Exit(putSerialLock);
			}
			return results.ToArray();
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
			Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(new BackupStatus
			{
				Started = SystemTime.UtcNow,
				IsRunning = true,
			}), new RavenJObject(), null);
			IndexStorage.FlushMapIndexes();
			IndexStorage.FlushReduceIndexes();
			TransactionalStorage.StartBackupOperation(this, backupDestinationDirectory, incrementalBackup, databaseDocument);
		}

		public static void Restore(RavenConfiguration configuration, string backupLocation, string databaseLocation, Action<string> output, bool defrag = true)
		{
			using (var transactionalStorage = configuration.CreateTransactionalStorage(() => { }))
			{
				transactionalStorage.Restore(backupLocation, databaseLocation, output, defrag);
			}
		}

		public byte[] PromoteTransaction(Guid fromTxId)
		{
			var committableTransaction = new CommittableTransaction();
			var transmitterPropagationToken = TransactionInterop.GetTransmitterPropagationToken(committableTransaction);
			TransactionalStorage.Batch(
				actions =>
					actions.Transactions.ModifyTransactionId(fromTxId, committableTransaction.TransactionInformation.DistributedIdentifier,
												TransactionManager.DefaultTimeout));
			promotedTransactions.TryAdd(committableTransaction.TransactionInformation.DistributedIdentifier, committableTransaction);
			return transmitterPropagationToken;
		}

		public void ResetIndex(string index)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			var indexDefinition = IndexDefinitionStorage.GetIndexDefinition(index);
			if (indexDefinition == null)
				throw new InvalidOperationException("There is no index named: " + index);
			DeleteIndex(index);
			PutIndex(index, indexDefinition);
		}

		public IndexDefinition GetIndexDefinition(string index)
		{
			index = IndexDefinitionStorage.FixupIndexName(index);
			return IndexDefinitionStorage.GetIndexDefinition(index);
		}

		static string buildVersion;
		public static string BuildVersion
		{
			get
			{
				return buildVersion ??
					   (buildVersion = FileVersionInfo.GetVersionInfo(typeof(DocumentDatabase).Assembly.Location).FileBuildPart.ToString(CultureInfo.InvariantCulture));
			}
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

		public TransportState TransportState { get; private set; }

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
			var indexes = Directory.GetFiles(Configuration.IndexStoragePath, "*.*", SearchOption.AllDirectories);
			var totalIndexSize = indexes.Sum(file => new FileInfo(file).Length);

			return totalIndexSize + TransactionalStorage.GetDatabaseSizeInBytes();
		}

		public Guid GetIndexEtag(string indexName, Guid? previousEtag)
		{
			Guid lastDocEtag = Guid.Empty;
			Guid? lastReducedEtag = null;
			bool isStale = false;
			int touchCount = 0;
			TransactionalStorage.Batch(accessor =>
			{
				isStale = accessor.Staleness.IsIndexStale(indexName, null, null);
				lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				lastReducedEtag = accessor.Staleness.GetMostRecentReducedEtag(indexName);
				touchCount = accessor.Staleness.GetIndexTouchCount(indexName);
			});


			var indexDefinition = GetIndexDefinition(indexName);
			if (indexDefinition == null)
				return Guid.NewGuid(); // this ensures that we will get the normal reaction of IndexNotFound later on.
			using (var md5 = MD5.Create())
			{
				var list = new List<byte>();
				list.AddRange(indexDefinition.GetIndexHash());
				list.AddRange(Encoding.Unicode.GetBytes(indexName));
				list.AddRange(lastDocEtag.ToByteArray());
				list.AddRange(BitConverter.GetBytes(touchCount));
				list.AddRange(BitConverter.GetBytes(isStale));
				if (lastReducedEtag != null)
				{
					list.AddRange(lastReducedEtag.Value.ToByteArray());
				}

				var indexEtag = new Guid(md5.ComputeHash(list.ToArray()));

				if (previousEtag != null && previousEtag != indexEtag)
				{
					// the index changed between the time when we got it and the time 
					// we actually call this, we need to return something random so that
					// the next time we won't get 304
					return Guid.NewGuid();
				}

				return indexEtag;
			}
		}

		public void AddAlert(Alert alert)
		{
			AlertsDocument alertsDocument;
			var alertsDoc = Get(Constants.RavenAlerts, null);
			if (alertsDoc == null)
				alertsDocument = new AlertsDocument();
			else
				alertsDocument = alertsDoc.DataAsJson.JsonDeserialization<AlertsDocument>() ?? new AlertsDocument();

			alertsDocument.Alerts.Add(alert);
		}
	}
}
