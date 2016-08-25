//-----------------------------------------------------------------------
// <copyright file="HttpServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using Jint;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Responders;
using Raven.Database.Util;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Plugins.Builtins.Tenants;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;

namespace Raven.Database.Server
{
	public class HttpServer : IDisposable
	{
		private readonly DateTime startUpTime = SystemTime.UtcNow;
		private readonly ConcurrentDictionary<string, DatabaseIdleTracking> lastWriteRequest = new ConcurrentDictionary<string, DatabaseIdleTracking>();
		private readonly SemaphoreSlim resourceCreationSemaphore;
		private readonly TimeSpan concurrentDatabaseLoadTimeout;
		private class DatabaseIdleTracking
		{
			public DateTime LastWrite;
			public DateTime LastIdle;
		}
		public DocumentDatabase SystemDatabase { get; private set; }
		public InMemoryRavenConfiguration SystemConfiguration { get; private set; }
		readonly MixedModeRequestAuthorizer requestAuthorizer;

		private readonly IBufferPool bufferPool = new BufferPool(BufferPoolStream.MaxBufferSize * 512, BufferPoolStream.MaxBufferSize);

		public event Action<InMemoryRavenConfiguration> SetupTenantDatabaseConfiguration = delegate { };

		private readonly ThreadLocal<string> currentTenantId = new ThreadLocal<string>();
		private readonly ThreadLocal<DocumentDatabase> currentDatabase = new ThreadLocal<DocumentDatabase>();
		private readonly ThreadLocal<InMemoryRavenConfiguration> currentConfiguration = new ThreadLocal<InMemoryRavenConfiguration>();

		protected readonly ConcurrentSet<string> LockedDatabases =
			new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

		protected readonly AtomicDictionary<Task<DocumentDatabase>> ResourcesStoresCache =
			new AtomicDictionary<Task<DocumentDatabase>>(StringComparer.OrdinalIgnoreCase);

		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly SemaphoreSlim _maxNumberOfThreadsForDatabaseToLoad;

		private readonly ConcurrentDictionary<string, TransportState> databaseTransportStates = new ConcurrentDictionary<string, TransportState>(StringComparer.OrdinalIgnoreCase);

#if DEBUG
		private readonly ConcurrentQueue<string> recentRequests = new ConcurrentQueue<string>();

		public ConcurrentQueue<string> RecentRequests
		{
			get { return recentRequests; }
		}
#endif

		public int NumberOfRequests
		{
			get { return Thread.VolatileRead(ref physicalRequestsCount); }
		}

		[ImportMany]
		public OrderedPartCollection<IConfigureHttpListener> ConfigureHttpListeners { get; set; }

		public InMemoryRavenConfiguration Configuration
		{
			get
			{
				return SystemConfiguration;
			}
		}

		private static readonly Regex databaseQuery = new Regex("^/databases/([^/]+)(?=/?)", RegexOptions.IgnoreCase);
		public static readonly Regex ChangesQuery = new Regex("^(/databases/([^/]+))?/changes/events", RegexOptions.IgnoreCase);

		private HttpListener listener;

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private int reqNum;


		// concurrent requests
		// we set 1/4 aside for handling background tasks
		private readonly SemaphoreSlim concurrentRequestSemaphore;
		private Timer serverTimer;
		private int physicalRequestsCount;

		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private bool disposed;

		public bool HasPendingRequests
		{
			get { return concurrentRequestSemaphore.CurrentCount != SystemConfiguration.MaxConcurrentServerRequests; }
		}

		public HttpServer(InMemoryRavenConfiguration configuration, DocumentDatabase resourceStore)
		{
			resourceCreationSemaphore = new SemaphoreSlim(configuration.MaxConcurrentDatabaseLoads);
			concurrentDatabaseLoadTimeout = configuration.ConcurrentDatabaseLoadTimeout;
            _maxNumberOfThreadsForDatabaseToLoad = new SemaphoreSlim(configuration.MaxConcurrentRequestsForDatabaseDuringLoad);
		    _maxSecondsForTaskToWaitForDatabaseToLoad = configuration.MaxSecondsForTaskToWaitForDatabaseToLoad;
			HttpEndpointRegistration.RegisterHttpEndpointTarget();
            ThreadPool.SetMinThreads(configuration.MinThreadPoolWorkerThreads, configuration.MinThreadPoolCompletionThreads);

            if (configuration.RunInMemory == false)
			{
				if (configuration.CreatePluginsDirectoryIfNotExisting)
				{
					TryCreateDirectory(configuration.PluginsDirectory);
				}
				if (configuration.CreateAnalyzersDirectoryIfNotExisting)
				{
					TryCreateDirectory(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Analyzers"));
				}
			}

			SystemDatabase = resourceStore;
			SystemConfiguration = configuration;

			concurrentRequestSemaphore = new SemaphoreSlim(SystemConfiguration.MaxConcurrentServerRequests);

			int val;
			if (int.TryParse(configuration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;
			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val);
			if (int.TryParse(configuration.Settings["Raven/Tenants/FrequencyToCheckForIdleDatabases"], out val) == false)
				val = 60;
			frequencyToCheckForIdleDatabases = TimeSpan.FromSeconds(val);

			configuration.Container.SatisfyImportsOnce(this);

			InitializeRequestResponders(SystemDatabase);

			requestAuthorizer = new MixedModeRequestAuthorizer();

			requestAuthorizer.Initialize(SystemDatabase, SystemConfiguration, () => currentTenantId.Value, this);

			foreach (var task in configuration.Container.GetExportedValues<IServerStartupTask>())
			{
				task.Execute(this);
			}
		}

		public MixedModeRequestAuthorizer RequestAuthorizer
		{
			get { return requestAuthorizer; }
		}

		private bool TryCreateDirectory(string path)
		{
			try
			{
				if (Directory.Exists(path) == false)
					Directory.CreateDirectory(path);
				return true;
			}
			catch (Exception e)
			{
				logger.WarnException("Could not create directory " + path, e);
				return false;
			}
		}

		private void InitializeRequestResponders(DocumentDatabase documentDatabase)
		{
			foreach (var responder in documentDatabase.RequestResponders)
			{
				responder.Value.Initialize(() => currentDatabase.Value, () => currentConfiguration.Value, () => currentTenantId.Value,
										   this);
			}
		}

		private void TenantDatabaseRemoved(object sender, TenantDatabaseModified.Event @event)
		{
			if (@event.Database != SystemDatabase)
				return; // we ignore anything that isn't from the root db

			logger.Info("Shutting down database {0} because the tenant database has been updated or removed", @event.Name);
			CleanupDatabase(@event.Name, skipIfActive: false);
		}

		public AdminStatistics Statistics
		{
			get
			{
				var activeDatabases = ResourcesStoresCache.Where(x => x.Value.Status == TaskStatus.RanToCompletion).Select(x => new
				{
					Name = x.Key,
					Database = x.Value.Result
				});
				var allDbs = activeDatabases.Concat(new[] { new { Name = Constants.SystemDatabase, Database = SystemDatabase } }).ToArray();
				return new AdminStatistics
				{
					ServerName = currentConfiguration.Value.ServerName,
					ClusterName = currentConfiguration.Value.ClusterName,
					TotalNumberOfRequests = NumberOfRequests,
					Uptime = SystemTime.UtcNow - startUpTime,
					Memory = new AdminMemoryStatistics
					{
						DatabaseCacheSizeInMB = ConvertBytesToMBs(SystemDatabase.TransactionalStorage.GetDatabaseCacheSizeInBytes()),
						ManagedMemorySizeInMB = ConvertBytesToMBs(GetCurrentManagedMemorySize()),
						TotalProcessMemorySizeInMB = ConvertBytesToMBs(GetCurrentProcessPrivateMemorySize64()),
					},
					LoadedDatabases =
						from documentDatabase in allDbs
						let indexStorageSize = documentDatabase.Database.GetIndexStorageSizeOnDisk()
						let transactionalStorageSize = documentDatabase.Database.GetTransactionalStorageSizeOnDisk()
						let totalDatabaseSize = indexStorageSize + transactionalStorageSize
						let lastUsed = documentDatabase.Database.WorkContext.LastWorkTime
						select new LoadedDatabaseStatistics
						{
							Name = documentDatabase.Name,
							LastActivity = lastUsed,
							TransactionalStorageSize = transactionalStorageSize,
							TransactionalStorageSizeHumaneSize = DatabaseSize.Humane(transactionalStorageSize),
							IndexStorageSize = indexStorageSize,
							IndexStorageHumaneSize = DatabaseSize.Humane(indexStorageSize),
							TotalDatabaseSize = totalDatabaseSize,
							TotalDatabaseHumaneSize = DatabaseSize.Humane(totalDatabaseSize),
							CountOfDocuments = documentDatabase.Database.Statistics.CountOfDocuments,
							RequestsPerSecond = Math.Round(documentDatabase.Database.WorkContext.PerformanceCounters.RequestsPerSecond.NextValue(), 2),
							ConcurrentRequests = (int)documentDatabase.Database.WorkContext.PerformanceCounters.ConcurrentRequests.NextValue(),
							DatabaseTransactionVersionSizeInMB = ConvertBytesToMBs(documentDatabase.Database.TransactionalStorage.GetDatabaseTransactionVersionSizeInBytes()),
						}
				};
			}
		}

		private decimal ConvertBytesToMBs(long bytes)
		{
			return Math.Round(bytes / 1024.0m / 1024.0m, 2);
		}

		private static long GetCurrentProcessPrivateMemorySize64()
		{
			using (var p = Process.GetCurrentProcess())
				return p.PrivateMemorySize64;
		}

		private static long GetCurrentManagedMemorySize()
		{
			var safelyGetPerformanceCounter = PerformanceCountersUtils.SafelyGetPerformanceCounter(
				".NET CLR Memory", "# Total committed Bytes", CurrentProcessName.Value);
			return safelyGetPerformanceCounter ?? GC.GetTotalMemory(false);
		}

		private static readonly Lazy<string> CurrentProcessName = new Lazy<string>(() =>
		{
			using (var p = Process.GetCurrentProcess())
				return p.ProcessName;
		});
	    private int _maxSecondsForTaskToWaitForDatabaseToLoad;

	    public void Dispose()
		{
			bool hasWriteLock = true;
			if (disposerLock.TryEnterWriteLock(TimeSpan.FromMinutes(2)) == false)
			{
				hasWriteLock = false;
				logger.Warn("After waiting for 2 minutes for disposer lock, giving up. Will do rude disposal");
			}
			try
			{
                TenantDatabaseModified.Occured -= TenantDatabaseRemoved;
				var exceptionAggregator = new ExceptionAggregator(logger, "Could not properly dispose of HttpServer");
				exceptionAggregator.Execute(resourceCreationSemaphore.Dispose);
				exceptionAggregator.Execute(() =>
				{
					foreach (var databaseTransportState in databaseTransportStates)
					{
						databaseTransportState.Value.Dispose();
					}
				});
				exceptionAggregator.Execute(() =>
				{
					if (serverTimer != null)
						serverTimer.Dispose();
				});
				exceptionAggregator.Execute(() =>
				{
					if (listener != null && listener.IsListening)
						listener.Close();
				});
				disposed = true;

				if (requestAuthorizer != null)
					exceptionAggregator.Execute(requestAuthorizer.Dispose);

				exceptionAggregator.Execute(() =>
				{
					using (ResourcesStoresCache.WithAllLocks())
					{
						// shut down all databases in parallel, avoid having to wait for each one
						Parallel.ForEach(ResourcesStoresCache.Values, dbTask =>
						{
							if (dbTask.IsCompleted == false)
							{
								dbTask.ContinueWith(task =>
								{
									if (task.Status != TaskStatus.RanToCompletion)
										return;

									try
									{
										logger.Info("Delayed shut down database {0} because we are shutting down the server", task.Result.Name);
										task.Result.Dispose();
									}
									catch (Exception e)
									{
										logger.WarnException("Failure in deferred disposal of a database", e);
									}
								});
							}
							else if (dbTask.Status == TaskStatus.RanToCompletion)
							{
								logger.Info("Shutting down database {0} because we are shutting down the server", dbTask.Result.Name);
								exceptionAggregator.Execute(dbTask.Result.Dispose);
							}
							// there is no else, the db is probably faulted
						});
						ResourcesStoresCache.Clear();
					}
				});

				exceptionAggregator.Execute(currentConfiguration.Dispose);
				exceptionAggregator.Execute(currentDatabase.Dispose);
				exceptionAggregator.Execute(currentTenantId.Dispose);
				exceptionAggregator.Execute(bufferPool.Dispose);
				exceptionAggregator.ThrowIfNeeded();
			}
			finally
			{
				if (hasWriteLock)
					disposerLock.ExitWriteLock();
			}
		}

		public void StartListening()
		{
			listener = new HttpListener();
			string virtualDirectory = SystemConfiguration.VirtualDirectory;
			if (virtualDirectory.EndsWith("/") == false)
				virtualDirectory = virtualDirectory + "/";
			var prefix = Configuration.UseSsl ? "https://" : "http://";
			var uri = prefix + (SystemConfiguration.HostName ?? "+") + ":" + SystemConfiguration.Port + virtualDirectory;
			listener.Prefixes.Add(uri);

			foreach (var configureHttpListener in ConfigureHttpListeners)
			{
				configureHttpListener.Value.Configure(listener, SystemConfiguration);
			}

			Init();
			listener.Start();

			Task.Factory.StartNew(async () =>
			{
				while (listener.IsListening)
				{
					HttpListenerContext context = null;
					try
					{
						context = await listener.GetContextAsync();
					}
					catch (ObjectDisposedException)
					{
						break;
					}
					catch (Exception)
					{
						continue;
					}

					ProcessRequest(context);
				}
			}, TaskCreationOptions.LongRunning);
		}

		private void ProcessRequest(HttpListenerContext context)
		{
			if (context == null)
				return;

			Task.Factory.StartNew(() =>
			{
				var ctx = new HttpListenerContextAdpater(context, SystemConfiguration, bufferPool);

				if (concurrentRequestSemaphore.Wait(TimeSpan.FromSeconds(5)) == false)
				{
					try
					{
						HandleTooBusyError(ctx);
					}
					catch (Exception e)
					{
						logger.WarnException("Could not send a too busy error to the client", e);
					}
					return;
				}
				try
				{
					Interlocked.Increment(ref physicalRequestsCount);
#if DEBUG
					recentRequests.Enqueue(ctx.Request.RawUrl);
					while (recentRequests.Count > 50)
					{
						string _;
						recentRequests.TryDequeue(out _);
					}
#endif

					if (ChangesQuery.IsMatch(ctx.GetRequestUrl()))
						HandleChangesRequest(ctx, () => { });
					else
						HandleActualRequest(ctx);
				}
				finally
				{
					concurrentRequestSemaphore.Release();
				}
			});
		}

		public void Init()
		{
			TenantDatabaseModified.Occured += TenantDatabaseRemoved;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
		}

		private readonly object runIdleOperationsLocker = new object();
		private void IdleOperations(object state)
		{
			if (Monitor.TryEnter(runIdleOperationsLocker, TimeSpan.FromSeconds(1)) == false)
				return;
			try
			{
				try
				{
					if (DatabaseHadRecentWritesRequests(SystemDatabase) == false)
						SystemDatabase.RunIdleOperations();
				}
				catch (Exception e)
				{
					logger.ErrorException("Error during idle operation run for system database", e);
				}

				var databasesToCleanup = new List<DocumentDatabase>();
				foreach (var documentDatabase in ResourcesStoresCache)
				{
					try
					{
						if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
							continue;
						var database = documentDatabase.Value.Result;
						if (DatabaseHadRecentWritesRequests(database) == false)
							database.RunIdleOperations();
						if ((SystemTime.UtcNow - database.WorkContext.LastWorkTime) > maxTimeDatabaseCanBeIdle)
							databasesToCleanup.Add(database);
					}
					catch (Exception e)
					{
						logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
					}
				}

				foreach (var db in databasesToCleanup)
				{
					logger.Info("Database {0}, had no incoming requests idle for {1}, trying to shut it down",
						db.Name, (SystemTime.UtcNow - db.WorkContext.LastWorkTime));

					// intentionally inside the loop, so we get better concurrency overall
					// since shutting down a database can take a while
					CleanupDatabase(db.Name, skipIfActive: true);

				}
			}
			finally
			{
				Monitor.Exit(runIdleOperationsLocker);
			}
		}

		private bool DatabaseHadRecentWritesRequests(DocumentDatabase db)
		{
			DatabaseIdleTracking lastWrite;
			if (lastWriteRequest.TryGetValue(db.Name ?? Constants.SystemDatabase, out lastWrite) == false)
				return false;
			var now = SystemTime.UtcNow;
			var recentWriteRequest = (now - lastWrite.LastWrite).TotalMinutes < 1;
			var lastIdleLessThanOneHourAgo = (now - lastWrite.LastIdle).TotalHours < 1;
			if (recentWriteRequest &&  lastIdleLessThanOneHourAgo)
					return true;
			lastWrite.LastIdle = now;
			return false;

		}

		protected void CleanupDatabase(string db, bool skipIfActive)
		{
			using (var locker = ResourcesStoresCache.TryWithAllLocks())
			{
				if (locker == null)
					return;

				DateTime time;
				Task<DocumentDatabase> databaseTask;
				if (ResourcesStoresCache.TryGetValue(db, out databaseTask) == false)
				{
					return;
				}

				if (databaseTask.Status != TaskStatus.RanToCompletion)
				{
					return; // still starting up
				}

				var database = databaseTask.Result;
				var isCurrentlyIndexing = database.IndexDefinitionStorage.IsCurrentlyIndexing();
				var lastWorkTime = database.WorkContext.LastWorkTime;
				if (skipIfActive &&
					((SystemTime.UtcNow - lastWorkTime) < maxTimeDatabaseCanBeIdle ||
					isCurrentlyIndexing))
				{
					logger.Info(
						"Will not be shutting down database {0} because is is doing work, last work at {1}, indexing: {2}",
						database.Name,
						lastWorkTime,
						isCurrentlyIndexing);
					// this document might not be actively working with user, but it is actively doing indexes, we will 
					// wait with unloading this database until it hasn't done indexing for a while.
					// This prevent us from shutting down big databases that have been left alone to do indexing work.
					return;
				}
				try
				{
					logger.Info("Shutting down database {0}. Last work time: {1}, skipIfActive: {2}",
						database.Name,
						lastWorkTime,
						skipIfActive
						);
					database.Dispose();
				}
				catch (Exception e)
				{
					logger.ErrorException("Could not cleanup tenant database: " + db, e);
					return;
				}

				var onDatabaseCleanupOccured = DatabaseCleanupOccured;
				if (onDatabaseCleanupOccured != null)
					onDatabaseCleanupOccured(db);
			}
		}

		public Task HandleChangesRequest(IHttpContext context, Action onDisconnect)
		{
			var sw = Stopwatch.StartNew();
			try
			{
				if (SetupRequestToProperDatabase(context) == false)
				{
					FinalizeRequestSafe(context);
					onDisconnect();
					return new CompletedTask();
				}

				if (!SetThreadLocalState(context))
				{
					FinalizeRequestSafe(context);
					onDisconnect();
					return new CompletedTask();
				}
				var eventsTransport = new EventsTransport(context);
				eventsTransport.Disconnected += onDisconnect;
				var handleChangesRequest = eventsTransport.ProcessAsync();
				CurrentDatabase.TransportState.Register(eventsTransport);
				return handleChangesRequest;
			}
			catch (Exception e)
			{
				try
				{
					ExceptionHandler.TryHandleException(context, e);
					LogException(e);
				}
				finally
				{
					FinalizeRequestSafe(context);
				}
				onDisconnect();
				return new CompletedTask();
			}
			finally
			{
				try
				{
					LogHttpRequestStats(new LogHttpRequestStatsParams(
											sw,
											context.Request.Headers,
											context.Request.HttpMethod,
											context.Response.StatusCode,
											context.Request.Url.PathAndQuery));
				}
				catch (Exception e)
				{
					logger.WarnException("Could not gather information to log request stats", e);
				}
				ResetThreadLocalState();
			}
		}

		private void LogException(Exception e)
		{
			if (!ShouldLogException(e))
				return;
			var je = e as JintException;
			if (je != null)
			{
				while (je.InnerException is JintException)
				{
					je = (JintException)je.InnerException;
				}
				logger.WarnException("Error on request", je);
			}
			else
			{
				logger.WarnException("Error on request", e);
			}
		}

		private void FinalizeRequestSafe(IHttpContext context)
		{
			try
			{
				FinalizeRequestProcessing(context, null, true);
			}
			catch (Exception e2)
			{
				logger.ErrorException("Could not finalize request properly", e2);
			}
		}

		public event EventHandler<BeforeRequestEventArgs> BeforeRequest;
		public event Action<string> DatabaseCleanupOccured;

		public void HandleActualRequest(IHttpContext ctx)
		{
			var isReadLockHeld = disposerLock.IsReadLockHeld;
			if (isReadLockHeld == false)
			{
				if (disposerLock.TryEnterReadLock(TimeSpan.FromSeconds(10)) == false)
				{
					ctx.SetStatusToNotAvailable();
					ctx.FinalizeResponse();
					logger.Warn("Could not enter disposer lock, probably disposing server, aborting request");
					return;
				}
			}
			try
			{
				if (disposed)
					return;

				var sw = Stopwatch.StartNew();
				bool ravenUiRequest = false;
				try
				{
					ravenUiRequest = DispatchRequest(ctx);
				}
				catch (Exception e)
				{
					ExceptionHandler.TryHandleException(ctx, e);
					if (ShouldLogException(e))
						logger.WarnException("Error on request", e);
				}
				finally
				{
					try
					{
						FinalizeRequestProcessing(ctx, sw, ravenUiRequest);
					}
					catch (Exception e)
					{
						logger.ErrorException("Could not finalize request properly", e);
					}
				}
			}
			finally
			{
				if (isReadLockHeld == false)
					disposerLock.ExitReadLock();
			}
		}

		private static bool IsWriteRequest(IHttpContext ctx)
		{
			return AbstractRequestAuthorizer.IsGetRequest(ctx.Request.HttpMethod, ctx.Request.Url.AbsoluteUri) ==
				   false;
		}

		protected bool ShouldLogException(Exception exception)
		{
			return exception is IndexDisabledException == false &&
				   exception is IndexDoesNotExistsException == false;

		}

		private void FinalizeRequestProcessing(IHttpContext ctx, Stopwatch sw, bool ravenUiRequest)
		{
			LogHttpRequestStatsParams logHttpRequestStatsParam = null;
			try
			{
				logHttpRequestStatsParam = new LogHttpRequestStatsParams(
					sw,
					ctx.Request.Headers,
					ctx.Request.HttpMethod,
					ctx.Response.StatusCode,
					ctx.Request.Url.PathAndQuery);
			}
			catch (Exception e)
			{
				logger.WarnException("Could not gather information to log request stats", e);
			}

			ctx.FinalizeResponse();

			if (ravenUiRequest || logHttpRequestStatsParam == null || sw == null)
				return;

			sw.Stop();

			LogHttpRequestStats(logHttpRequestStatsParam);
			ctx.OutputSavedLogItems(logger);
		}

		private void LogHttpRequestStats(LogHttpRequestStatsParams logHttpRequestStatsParams)
		{
			if (logger.IsDebugEnabled == false)
				return;

			// we filter out requests for the UI because they fill the log with information
			// we probably don't care about them anyway. That said, we do output them if they take too
			// long.
			if (logHttpRequestStatsParams.Headers["Raven-Timer-Request"] == "true" &&
				logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds <= 25)
				return;

			var curReq = Interlocked.Increment(ref reqNum);
			logger.Debug("Request #{0,4:#,0}: {1,-7} - {2,5:#,0} ms - {5,-10} - {3} - {4}",
							   curReq,
							   logHttpRequestStatsParams.HttpMethod,
							   logHttpRequestStatsParams.Stopwatch.ElapsedMilliseconds,
							   logHttpRequestStatsParams.ResponseStatusCode,
							   logHttpRequestStatsParams.RequestUri,
							   currentTenantId.Value);
		}

		private static void HandleTooBusyError(IHttpContext ctx)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			ExceptionHandler.SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = "The server is too busy, could not acquire transactional access"
			});
		}

		private bool DispatchRequest(IHttpContext ctx)
		{
			Action onResponseEnd = null;

			if (SetupRequestToProperDatabase(ctx) == false)
			{
				return false;
			}
			try
			{
				if (!SetThreadLocalState(ctx))
					return false;

				OnDispatchingRequest(ctx);

				if (SystemConfiguration.HttpCompression)
					AddHttpCompressionIfClientCanAcceptIt(ctx);

				HandleHttpCompressionFromClient(ctx);

				if (BeforeDispatchingRequest != null)
				{
					onResponseEnd = BeforeDispatchingRequest(ctx);
				}

				// Cross-Origin Resource Sharing (CORS) is documented here: http://www.w3.org/TR/cors/
				AddAccessControlHeaders(ctx);
				if (ctx.Request.HttpMethod == "OPTIONS")
					return false;

				foreach (var requestResponderLazy in currentDatabase.Value.RequestResponders)
				{
					var requestResponder = requestResponderLazy.Value;
					if (requestResponder.WillRespond(ctx))
					{
						var sp = Stopwatch.StartNew();
						requestResponder.ReplicationAwareRespond(ctx);
						sp.Stop();
						if (ctx.Response.BufferOutput)
						{
							ctx.Response.AddHeader("Temp-Request-Time",
								sp.ElapsedMilliseconds.ToString("#,#;;0", CultureInfo.InvariantCulture));
						}
						return requestResponder.IsUserInterfaceRequest;
					}
				}
				ctx.SetStatusToBadRequest();
				if (ctx.Request.HttpMethod == "HEAD")
					return false;
				ctx.Write(
					@"
<html>
	<body>
		<h1>Could not figure out what to do</h1>
		<p>Your request didn't match anything that Raven knows to do, sorry...</p>
	</body>
</html>
");
			}
			finally
			{
				CurrentDatabase.WorkContext.PerformanceCounters.ConcurrentRequests.Decrement();
				ResetThreadLocalState();
				if (onResponseEnd != null)
					onResponseEnd();
			}
			return false;
		}

		private bool SetThreadLocalState(IHttpContext ctx)
		{
			CurrentOperationContext.Headers.Value = new NameValueCollection(ctx.Request.Headers);
			CurrentOperationContext.Headers.Value[Constants.RavenAuthenticatedUser] = "";
			CurrentOperationContext.User.Value = null;
			LogContext.DatabaseName.Value = CurrentDatabase.Name;
			var dbName = CurrentDatabase.Name ?? Constants.SystemDatabase;
			if (IsWriteRequest(ctx))
			{
				var databaseIdleTracking = lastWriteRequest.GetOrAdd(dbName, s => new DatabaseIdleTracking());
				databaseIdleTracking.LastWrite = SystemTime.UtcNow;
			}
			var disposable = LogManager.OpenMappedContext("database", dbName);
			CurrentOperationContext.RequestDisposables.Value.Add(disposable);
			if (ctx.RequiresAuthentication &&
				requestAuthorizer.Authorize(ctx) == false)
				return false;
			return true;
		}

		private void ResetThreadLocalState()
		{
			try
			{
				CurrentOperationContext.Headers.Value = new NameValueCollection();
				CurrentOperationContext.User.Value = null;
				LogContext.DatabaseName.Value = null;
				foreach (var disposable in CurrentOperationContext.RequestDisposables.Value)
				{
					disposable.Dispose();
				}
				CurrentOperationContext.RequestDisposables.Value.Clear();
				currentDatabase.Value = SystemDatabase;
				currentConfiguration.Value = SystemConfiguration;
			}
			catch
			{
				// this can happen during system shutdown
			}
		}

		public Func<IHttpContext, Action> BeforeDispatchingRequest { get; set; }

		private static void HandleHttpCompressionFromClient(IHttpContext ctx)
		{
			var encoding = ctx.Request.Headers["Content-Encoding"];
			if (encoding == null)
				return;

			if (encoding.Contains("gzip"))
			{
				ctx.SetRequestFilter(stream => new GZipStream(stream, CompressionMode.Decompress));
			}
			else if (encoding.Contains("deflate"))
			{
				ctx.SetRequestFilter(stream => new DeflateStream(stream, CompressionMode.Decompress));
			}
		}

		protected void OnDispatchingRequest(IHttpContext ctx)
		{
			ctx.Response.AddHeader("Raven-Server-Build", DocumentDatabase.BuildVersion);
			CurrentDatabase.WorkContext.PerformanceCounters.RequestsPerSecond.Increment();
			CurrentDatabase.WorkContext.PerformanceCounters.ConcurrentRequests.Increment();
		}

		public DocumentDatabase CurrentDatabase
		{
			get { return currentDatabase.Value ?? SystemDatabase; }
		}

		private bool SetupRequestToProperDatabase(IHttpContext ctx)
		{
			var requestUrl = ctx.GetRequestUrlForTenantSelection();
			var match = databaseQuery.Match(requestUrl);
			var onBeforeRequest = BeforeRequest;
			if (match.Success == false)
			{
				currentTenantId.Value = Constants.SystemDatabase;
				currentDatabase.Value = SystemDatabase;
				currentConfiguration.Value = SystemConfiguration;
				SystemDatabase.WorkContext.UpdateFoundWork();
				if (onBeforeRequest != null)
				{
					var args = new BeforeRequestEventArgs
					{
						Context = ctx,
						IgnoreRequest = false,
						TenantId = "System",
						Database = SystemDatabase
					};
					onBeforeRequest(this, args);
					if (args.IgnoreRequest)
						return false;
				}

				return true;
			}
			var tenantId = match.Groups[1].Value;
			Task<DocumentDatabase> resourceStoreTask = null;
			bool hasSemaphore = false;
			bool hasDb;
			try
			{
				hasSemaphore = resourceCreationSemaphore.Wait(concurrentDatabaseLoadTimeout);
				if (hasSemaphore == false)
				{
					HandleTooMuchDatabasesAtOnce(ctx, tenantId);
					return false;
				}
				hasDb = TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
			}
			catch (Exception e)
			{
				OutputDatabaseOpenFailure(ctx, tenantId, e);
				return false;
			}
			finally
			{
				if (hasSemaphore)
					resourceCreationSemaphore.Release();
			}

			if (hasDb)
			{
				try
				{
					if (resourceStoreTask == null) //precaution, should never happen
					{
						ctx.SetStatusToBadRequest();
						ctx.WriteJson(new
						{
							Error =
								string.Format(
									"The database {0} failed to load. Something bad has happened, this error should be reported as it is probably a bug.",
									tenantId),
						});
						return false;
					}

					if (resourceStoreTask.IsCompleted == false && resourceStoreTask.IsFaulted == false)
					{
						if (_maxNumberOfThreadsForDatabaseToLoad.Wait(0) == false)
						{
							ctx.SetStatusToNotAvailable();
							ctx.WriteJson(new
							{
								Error =
									string.Format(
										"The database {0} is currently being loaded, but there are too many requests waiting for database load. Please try again later, database loading continues.",
										tenantId),
							});
							return false;
						}
						try
						{
                            if (resourceStoreTask.Wait(TimeSpan.FromSeconds(_maxSecondsForTaskToWaitForDatabaseToLoad)) == false)
							{
								ctx.SetStatusToNotAvailable();
								ctx.WriteJson(new
								{
									Error =
										string.Format(
											"The database {0} is currently being loaded, but after {1} seconds, this request has been aborted. Please try again later, database loading continues.",
                                            tenantId, _maxSecondsForTaskToWaitForDatabaseToLoad),
								});
								return false;
							}
						}
						finally
						{
							_maxNumberOfThreadsForDatabaseToLoad.Release();
						}
					}
					if (onBeforeRequest != null)
					{
						var args = new BeforeRequestEventArgs
						{
							Context = ctx,
							IgnoreRequest = false,
							TenantId = tenantId,
							Database = resourceStoreTask.Result
						};
						onBeforeRequest(this, args);
						if (args.IgnoreRequest)
							return false;
					}
				}
				catch (Exception e)
				{
					OutputDatabaseOpenFailure(ctx, tenantId, e);
					return false;
				}
				var resourceStore = resourceStoreTask.Result;

				resourceStore.WorkContext.UpdateFoundWork();

				if (string.IsNullOrEmpty(Configuration.VirtualDirectory) == false && Configuration.VirtualDirectory != "/")
				{
					ctx.AdjustUrl(Configuration.VirtualDirectory + match.Value);
				}
				else
				{
					ctx.AdjustUrl(match.Value);
				}
				currentTenantId.Value = tenantId;
				currentDatabase.Value = resourceStore;
				currentConfiguration.Value = resourceStore.Configuration;
			}
			else
			{
				ctx.SetStatusToNotFound();
				ctx.WriteJson(new
				{
					Error = "Could not find a database named: " + tenantId
				});
				return false;
			}

			return true;
		}

		private void HandleTooMuchDatabasesAtOnce(IHttpContext ctx, string tenantId)
		{
			ctx.SetStatusToNotAvailable();
			var msg = string.Format(
				"The database {0} cannot be loaded, there are currently {1} databases being loaded and we already waited for {2} for them to finish. Try again later",
				tenantId,
				resourceCreationSemaphore.CurrentCount,
				concurrentDatabaseLoadTimeout);
			logger.Warn(msg);
			ctx.WriteJson(new
			{
				Error =
					msg,
			});
		}

		private static void OutputDatabaseOpenFailure(IHttpContext ctx, string tenantId, Exception e)
		{
			var msg = "Could not open database named: " + tenantId;
			logger.WarnException(msg, e);
			ctx.SetStatusToNotAvailable();
			ctx.WriteJson(new
			{
				Error = msg,
				Reason = e.ToString()
			});
		}

		public void LockDatabase(string tenantId, Action actionToTake)
		{
			if (LockedDatabases.TryAdd(tenantId) == false)
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");
			try
			{
				logger.Info("Shutting down database {0} because we have been ordered to lock the db", tenantId);
				CleanupDatabase(tenantId, false);
				actionToTake();
			}
			finally
			{
				LockedDatabases.TryRemove(tenantId);
			}

		}

		public void ForAllDatabases(Action<DocumentDatabase> action)
		{
			action(SystemDatabase);
			foreach (var value in ResourcesStoresCache
				.Select(db => db.Value)
				.Where(value => value.Status == TaskStatus.RanToCompletion))
			{
				action(value.Result);
			}
		}

		protected bool TryGetOrCreateResourceStore(string tenantId, out Task<DocumentDatabase> database)
		{
			if (ResourcesStoresCache.TryGetValue(tenantId, out database))
			{
				if (database.IsFaulted && database.Exception != null)
				{
					// if we are here, there is an error, and if there is an error, we need to clear it from the 
					// resource store cache so we can try to reload it.
					// Note that we return the faulted task anyway, because we need the user to look at the error
					if (database.Exception.Data.Contains("Raven/KeepInResourceStore") == false)
					{
						Task<DocumentDatabase> val;
						ResourcesStoresCache.TryRemove(tenantId, out val);
					}
				}

				return true;
			}

			if (LockedDatabases.Contains(tenantId))
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");

			var config = CreateTenantConfiguration(tenantId);
			if (config == null)
				return false;
            
            if (resourceCreationSemaphore.Wait(concurrentDatabaseLoadTimeout) == false)
            {
                var msg = string.Format(
                                "The database {0} cannot be loaded, there are currently {1} databases being loaded and we already waited for {2} for them to finish. Try again later",
                                tenantId,
                                resourceCreationSemaphore.CurrentCount,
                                concurrentDatabaseLoadTimeout);
                logger.Warn(msg);
                database= null;
                return false;
            }

		    var newTaskCreated = false;

            database = ResourcesStoresCache.GetOrAdd(tenantId, __ =>
            {
                newTaskCreated = true;
                return Task.Factory.StartNew(() =>
                {

                    var transportState = databaseTransportStates.GetOrAdd(tenantId, s => new TransportState());
                    var documentDatabase = new DocumentDatabase(config, transportState);
                    try
                    {
                        AssertLicenseParameters(config);
                        documentDatabase.SpinBackgroundWorkers();
                        InitializeRequestResponders(documentDatabase);

                        // if we have a very long init process, make sure that we reset the last idle time for this db.
                        documentDatabase.WorkContext.UpdateFoundWork();
                    }
                    catch (Exception)
                    {
                        documentDatabase.Dispose();
                        throw;
                    }
                    documentDatabase.Disposing += DocumentDatabaseDisposingStarted;
                    documentDatabase.DisposingEnded += DocumentDatabaseDisposingEnded;
                    return documentDatabase;
                }).ContinueWith(task =>
                {
                    if (task.Status == TaskStatus.Faulted) // this observes the task exception
                    {
                        logger.WarnException("Failed to create database " + tenantId, task.Exception);
                    }
                    resourceCreationSemaphore.Release();
                    return task;
                }).Unwrap();
            });


            if (newTaskCreated == false)
                resourceCreationSemaphore.Release();
            return true;
		}

		private void DocumentDatabaseDisposingStarted(object documentDatabase, EventArgs args)
		{
			try
			{
				var database = documentDatabase as DocumentDatabase;
				if (database == null || disposed)
				{
					return;
				}

				ResourcesStoresCache.Set(database.Name, (dbName) =>
				{
					var tcs = new TaskCompletionSource<DocumentDatabase>();
					tcs.SetException(new ObjectDisposedException(dbName, "Database named " + database.Name + " is being disposed right now and cannot be accessed.\r\n" +
																 "Access will be available when the dispose process will end")
					{
						Data =
						{
							{"Raven/KeepInResourceStore", "true"}
						}
					});
					// we need to observe this task exception in case no one is actually looking at it during disposal
					GC.KeepAlive(tcs.Task.Exception);
					return tcs.Task;
				});
			}
			catch (Exception ex)
			{
				logger.WarnException("Failed to substitute database task with temporary place holder. This should not happen", ex);
			}

		}

		private void DocumentDatabaseDisposingEnded(object documentDatabase, EventArgs args)
		{
			try
			{
				var database = documentDatabase as DocumentDatabase;
				if (database == null || disposed)
				{
					return;
				}

				ResourcesStoresCache.Remove(database.Name);
			}
			catch (Exception ex)
			{
				logger.ErrorException("Failed to remove database at the end of the disposal. This should not happen", ex);
			}

		}

		private void AssertLicenseParameters(InMemoryRavenConfiguration config)
		{
			string maxDatabases;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfDatabases", out maxDatabases))
			{
				if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
				{
					var numberOfAllowedDbs = int.Parse(maxDatabases);

					var databases = SystemDatabase.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, numberOfAllowedDbs, CancellationToken.None).ToList();
					if (databases.Count >= numberOfAllowedDbs)
						throw new InvalidOperationException(
							"You have reached the maximum number of databases that you can have according to your license: " + numberOfAllowedDbs + Environment.NewLine +
							"You can either upgrade your RavenDB license or delete a database from the server");
				}
			}

			var bundles = config.Settings["Raven/ActiveBundles"];
			if (string.IsNullOrWhiteSpace(bundles) == false)
			{
				var bundlesList = bundles.Split(';').ToList();

				// We explicitly don't want to fail here for missing bundle if the user
				// has an valid license that expired, for example. We only perform the check
				// if the user has a _valid_ license that doesn't have the specified bundle
				if (ValidateLicense.CurrentLicense.Error)
					return;
				foreach (var bundle in bundlesList.Where(s => string.IsNullOrWhiteSpace(s) == false && s != "PeriodicBackup"))
				{
					string value;
					if (ValidateLicense.CurrentLicense.Attributes.TryGetValue(bundle, out value))
					{
						bool active;
						if (bool.TryParse(value, out active) && active == false)
							throw new InvalidOperationException("Your license does not allow the use of the " + bundle + " bundle.");
					}
				}
			}
		}

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId)
		{
			var document = GetTenantDatabaseDocument(tenantId);
			if (document == null)
				return null;

			var config = new InMemoryRavenConfiguration
			{
				Settings = new NameValueCollection(SystemConfiguration.Settings),
			};

			SetupTenantDatabaseConfiguration(config);

			config.CustomizeValuesForTenant(tenantId);


			foreach (var setting in document.Settings)
			{
				config.Settings[setting.Key] = setting.Value;
			}
			Unprotect(document);

			foreach (var securedSetting in document.SecuredSettings)
			{
				config.Settings[securedSetting.Key] = securedSetting.Value;
			}

			var dataDir = document.Settings["Raven/DataDir"];
			if (dataDir.StartsWith("~/") || dataDir.StartsWith(@"~\"))
			{
				var baseDataPath = Path.GetDirectoryName(SystemDatabase.Configuration.DataDirectory);
				if (baseDataPath == null)
					throw new InvalidOperationException("Could not find root data path");
				config.Settings["Raven/DataDir"] = Path.Combine(baseDataPath, dataDir.Substring(2));
			}
			config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

			config.DatabaseName = tenantId;
			config.IsTenantDatabase = true;

			config.Initialize();
			config.CopyParentSettings(SystemConfiguration);
			return config;
		}

		private DatabaseDocument GetTenantDatabaseDocument(string tenantId)
		{
			JsonDocument jsonDocument;
			using (SystemDatabase.DisableAllTriggersForCurrentThread())
				jsonDocument = SystemDatabase.Get("Raven/Databases/" + tenantId, null);
			if (jsonDocument == null ||
				jsonDocument.Metadata == null ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
				jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
				return null;

			var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (document.Settings["Raven/DataDir"] == null)
				throw new InvalidOperationException("Could not find Raven/DataDir");

			if (document.Disabled)
				throw new InvalidOperationException("The database has been disabled.");

			return document;
		}


		private void AddAccessControlHeaders(IHttpContext ctx)
		{
			if (string.IsNullOrEmpty(SystemConfiguration.AccessControlAllowOrigin))
				return;

			ctx.Response.AddHeader("Access-Control-Allow-Credentials", "true");

			bool originAllowed = SystemConfiguration.AccessControlAllowOrigin == "*" ||
					SystemConfiguration.AccessControlAllowOrigin.Split(' ')
						.Any(o => o == ctx.Request.Headers["Origin"]);
			if (originAllowed)
			{
				ctx.Response.AddHeader("Access-Control-Allow-Origin", ctx.Request.Headers["Origin"]);
			}

			ctx.Response.AddHeader("Access-Control-Max-Age", SystemConfiguration.AccessControlMaxAge);
			ctx.Response.AddHeader("Access-Control-Allow-Methods", SystemConfiguration.AccessControlAllowMethods);
			if (string.IsNullOrEmpty(SystemConfiguration.AccessControlRequestHeaders))
			{
				// allow whatever headers are being requested
				var hdr = ctx.Request.Headers["Access-Control-Request-Headers"]; // typically: "x-requested-with"
				if (hdr != null) ctx.Response.AddHeader("Access-Control-Allow-Headers", hdr);
			}
			else
			{
				ctx.Response.AddHeader("Access-Control-Request-Headers", SystemConfiguration.AccessControlRequestHeaders);
			}
		}

		private static void AddHttpCompressionIfClientCanAcceptIt(IHttpContext ctx)
		{
			var acceptEncoding = ctx.Request.Headers["Accept-Encoding"];

			if (string.IsNullOrEmpty(acceptEncoding))
				return;

			// The Studio xap is already a compressed file, it's a waste of time to try to compress it further.
			var requestUrl = ctx.GetRequestUrl();
			if (String.Equals(requestUrl, "/silverlight/Raven.Studio.xap", StringComparison.OrdinalIgnoreCase))
				return;

			// gzip must be first, because chrome has an issue accepting deflate data
			// when sending it json text
			if ((acceptEncoding.IndexOf("gzip", StringComparison.OrdinalIgnoreCase) != -1))
			{
				ctx.SetResponseFilter(s => new GZipStream(s, CompressionMode.Compress, true));
				ctx.Response.AddHeader("Content-Encoding", "gzip");
			}
			else if (acceptEncoding.IndexOf("deflate", StringComparison.OrdinalIgnoreCase) != -1)
			{
				ctx.SetResponseFilter(s => new DeflateStream(s, CompressionMode.Compress, true));
				ctx.Response.AddHeader("Content-Encoding", "deflate");
			}

		}

		public void ResetNumberOfRequests()
		{
			Interlocked.Exchange(ref reqNum, 0);
			Interlocked.Exchange(ref physicalRequestsCount, 0);
#if DEBUG
			while (recentRequests.Count > 0)
			{
				string _;
				recentRequests.TryDequeue(out _);
			}
#endif
		}

		public Task<DocumentDatabase> GetDatabaseInternal(string name)
		{
			if (string.Equals("System", name, StringComparison.OrdinalIgnoreCase))
				return new CompletedTask<DocumentDatabase>(SystemDatabase);
            
			Task<DocumentDatabase> db;
			if (TryGetOrCreateResourceStore(name, out db))
				return db;
			return null;
			
		}

		public void Protect(DatabaseDocument databaseDocument)
		{
			if (databaseDocument.SecuredSettings == null)
			{
				databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
				return;
			}

			foreach (var prop in databaseDocument.SecuredSettings.ToList())
			{
				if (prop.Value == null)
					continue;
				var bytes = Encoding.UTF8.GetBytes(prop.Value);
				var entrophy = Encoding.UTF8.GetBytes(prop.Key);
				var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
				databaseDocument.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
			}
		}

		public void Unprotect(DatabaseDocument databaseDocument)
		{
			if (databaseDocument.SecuredSettings == null)
			{
				databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
				return;
			}

			foreach (var prop in databaseDocument.SecuredSettings.ToList())
			{
				if (prop.Value == null)
					continue;
				var bytes = Convert.FromBase64String(prop.Value);
				var entrophy = Encoding.UTF8.GetBytes(prop.Key);
				try
				{
					var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
					databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
				}
				catch (Exception e)
				{
					logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
					databaseDocument.SecuredSettings[prop.Key] = "<data could not be decrypted>";
				}
			}
		}


		static class ExceptionHandler
		{
			private static readonly Dictionary<Type, Action<IHttpContext, Exception>> handlers =
				new Dictionary<Type, Action<IHttpContext, Exception>>
			{
				{typeof (BadRequestException), (ctx, e) => HandleBadRequest(ctx, e as BadRequestException)},
				{typeof (ConcurrencyException), (ctx, e) => HandleConcurrencyException(ctx, e as ConcurrencyException)},
				{typeof (JintException), (ctx, e) => HandleJintException(ctx, e as JintException)},
				{typeof (IndexDisabledException), (ctx, e) => HandleIndexDisabledException(ctx, e as IndexDisabledException)},
				{typeof (IndexDoesNotExistsException), (ctx, e) => HandleIndexDoesNotExistsException(ctx, e as IndexDoesNotExistsException)},
			};

			internal static void TryHandleException(IHttpContext ctx, Exception e)
			{
				var exceptionType = e.GetType();

				try
				{
					if (handlers.ContainsKey(exceptionType))
					{
						handlers[exceptionType](ctx, e);
						return;
					}

					var baseType = handlers.Keys.FirstOrDefault(t => t.IsInstanceOfType(e));
					if (baseType != null)
					{
						handlers[baseType](ctx, e);
						return;
					}

					DefaultHandler(ctx, e);
				}
				catch (Exception)
				{
					logger.ErrorException("Failed to properly handle error, further error handling is ignored", e);
				}
			}

			public static void SerializeError(IHttpContext ctx, object error)
			{
				var sw = new StreamWriter(ctx.Response.OutputStream);
				JsonExtensions.CreateDefaultJsonSerializer().Serialize(new JsonTextWriter(sw)
				{
					Formatting = Formatting.Indented,
				}, error);
				sw.Flush();
			}

			private static void DefaultHandler(IHttpContext ctx, Exception e)
			{
				ctx.Response.StatusCode = 500;
				ctx.Response.StatusDescription = "Internal Server Error";
				SerializeError(ctx, new
				{
					//ExceptionType = e.GetType().AssemblyQualifiedName,					
					Url = ctx.Request.RawUrl,
					Error = e.ToString(),
				});
			}

			private static void HandleBadRequest(IHttpContext ctx, BadRequestException e)
			{
				ctx.SetStatusToBadRequest();
				SerializeError(ctx, new
				{
					Url = ctx.Request.RawUrl,
					e.Message,
					Error = e.Message
				});
			}

			private static void HandleConcurrencyException(IHttpContext ctx, ConcurrencyException e)
			{
				ctx.Response.StatusCode = 409;
				ctx.Response.StatusDescription = "Conflict";
				SerializeError(ctx, new
				{
					Url = ctx.Request.RawUrl,
					ActualETag = e.ActualETag ?? Etag.Empty,
					ExpectedETag = e.ExpectedETag ?? Etag.Empty,
					Error = e.Message
				});
			}

			private static void HandleJintException(IHttpContext ctx, JintException e)
			{
				while (e.InnerException is JintException)
				{
					e = (JintException)e.InnerException;
				}

				ctx.SetStatusToBadRequest();
				SerializeError(ctx, new
				{
					Url = ctx.Request.RawUrl,
					Error = e.Message
				});
			}

			private static void HandleIndexDoesNotExistsException(IHttpContext ctx, IndexDoesNotExistsException e)
			{
				ctx.SetStatusToNotFound();
				SerializeError(ctx, new
				{
					Url = ctx.Request.RawUrl,
					Error = e.Message
				});
			}

			private static void HandleIndexDisabledException(IHttpContext ctx, IndexDisabledException e)
			{
				ctx.Response.StatusCode = 503;
				ctx.Response.StatusDescription = "Service Unavailable";
				SerializeError(ctx, new
				{
					Url = ctx.Request.RawUrl,
					Error = e.Information.GetErrorMessage(),
					Index = e.Information.Name,
				});
			}

		}
	}
}
