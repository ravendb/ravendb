using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Owin;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins.Builtins.Tenants;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi;
using Raven.Database.Util;

namespace Raven.Database.Server.Tenancy
{
	public class DatabasesLandlord : IDisposable
	{
		private readonly InMemoryRavenConfiguration systemConfiguration;
		private readonly DocumentDatabase systemDatabase;
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

		public event Action<InMemoryRavenConfiguration> SetupTenantDatabaseConfiguration = delegate { };
		public event Action<string> DatabaseCleanupOccured;
		public event EventHandler<BeforeRequestEventArgs> BeforeRequest;

		public readonly AtomicDictionary<Task<DocumentDatabase>> ResourcesStoresCache =
			new AtomicDictionary<Task<DocumentDatabase>>(StringComparer.OrdinalIgnoreCase);
		public readonly ConcurrentDictionary<string, DateTime> DatabaseLastRecentlyUsed = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

		protected readonly ConcurrentSet<string> LockedDatabases = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

		public DatabasesLandlord(DocumentDatabase systemDatabase)
		{
			systemConfiguration = systemDatabase.Configuration;
			this.systemDatabase = systemDatabase;

			int val;
			if (int.TryParse(systemConfiguration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;
			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val);
			if (int.TryParse(systemConfiguration.Settings["Raven/Tenants/FrequencyToCheckForIdleDatabases"], out val) == false)
				val = 60;
			frequencyToCheckForIdleDatabases = TimeSpan.FromSeconds(val);

			Init();
		}

		public DocumentDatabase SystemDatabase
		{
			get { return systemDatabase; }
		}

		public InMemoryRavenConfiguration SystemConfiguration
		{
			get { return systemConfiguration; }
		}

		public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId)
		{
			var document = GetTenantDatabaseDocument(tenantId);
			if (document == null)
				return null;

			var config = new InMemoryRavenConfiguration
			{
				Settings = new NameValueCollection(systemConfiguration.Settings),
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
				var baseDataPath = Path.GetDirectoryName(systemDatabase.Configuration.DataDirectory);
				if (baseDataPath == null)
					throw new InvalidOperationException("Could not find root data path");
				config.Settings["Raven/DataDir"] = Path.Combine(baseDataPath, dataDir.Substring(2));
			}
			config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

			config.DatabaseName = tenantId;
			config.IsTenantDatabase = true;

			config.Initialize();
			config.CopyParentSettings(systemConfiguration);
			return config;
		}

		private DatabaseDocument GetTenantDatabaseDocument(string tenantId)
		{
			JsonDocument jsonDocument;
			using (systemDatabase.DisableAllTriggersForCurrentThread())
				jsonDocument = systemDatabase.Get("Raven/Databases/" + tenantId, null);
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
					Logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
					databaseDocument.SecuredSettings[prop.Key] = "<data could not be decrypted>";
				}
			}
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

		public void LockDatabase(string tenantId, Action actionToTake)
		{
			if (LockedDatabases.TryAdd(tenantId) == false)
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");
			try
			{
				CleanupDatabase(tenantId, false);
				actionToTake();
			}
			finally
			{
				LockedDatabases.TryRemove(tenantId);
			}
		}

		public Task<DocumentDatabase> GetDatabaseInternal(string name)
		{
			if (string.Equals("<system>", name, StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(name))
				return new CompletedTask<DocumentDatabase>(systemDatabase);

			Task<DocumentDatabase> db;
			if (TryGetOrCreateResourceStore(name, out db))
				return db;
			return Task.FromResult<DocumentDatabase>(null);
		}

		public void CleanupDatabase(string db, bool skipIfActive)
		{
			using (ResourcesStoresCache.WithAllLocks())
			{
				DateTime time;
				Task<DocumentDatabase> databaseTask;
				if (ResourcesStoresCache.TryGetValue(db, out databaseTask) == false)
				{
					DatabaseLastRecentlyUsed.TryRemove(db, out time);
					return;
				}
				if (databaseTask.Status == TaskStatus.Faulted || databaseTask.Status == TaskStatus.Canceled)
				{
					DatabaseLastRecentlyUsed.TryRemove(db, out time);
					ResourcesStoresCache.TryRemove(db, out databaseTask);
					return;
				}
				if (databaseTask.Status != TaskStatus.RanToCompletion)
				{
					return; // still starting up
				}

				var database = databaseTask.Result;
				if (skipIfActive &&
					(SystemTime.UtcNow - database.WorkContext.LastWorkTime).TotalMinutes < 10)
				{
					// this document might not be actively working with user, but it is actively doing indexes, we will 
					// wait with unloading this database until it hasn't done indexing for a while.
					// This prevent us from shutting down big databases that have been left alone to do indexing work.
					return;
				}
				try
				{
					database.Dispose();
				}
				catch (Exception e)
				{
					Logger.ErrorException("Could not cleanup tenant database: " + db, e);
					return;
				}
				DatabaseLastRecentlyUsed.TryRemove(db, out time);
				ResourcesStoresCache.TryRemove(db, out databaseTask);

				var onDatabaseCleanupOccured = DatabaseCleanupOccured;
				if (onDatabaseCleanupOccured != null)
					onDatabaseCleanupOccured(db);
			}
		}

		public void TenantDatabaseRemoved(object sender, TenantDatabaseModified.Event @event)
		{
			if (@event.Database != systemDatabase)
				return; // we ignore anything that isn't from the root db

			CleanupDatabase(@event.Name, skipIfActive: false);
		}

		private bool SetupRequestToProperDatabase(string tenantId)
		{
			var onBeforeRequest = BeforeRequest;
			if (string.IsNullOrWhiteSpace(tenantId))
			{
				DatabaseLastRecentlyUsed.AddOrUpdate("System", SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
				if (onBeforeRequest != null)
				{
					var args = new BeforeRequestEventArgs
					{
						IgnoreRequest = false,
						TenantId = "System",
						Database = systemDatabase
					};
					onBeforeRequest(this, args);
					if (args.IgnoreRequest)
						return false;
				}

				return true;
			}

			Task<DocumentDatabase> resourceStoreTask;
			bool hasDb;
			try
			{
				hasDb = TryGetOrCreateResourceStore(tenantId, out resourceStoreTask);
			}
			catch (Exception e)
			{
				OutputDatabaseOpenFailure(tenantId, e);
				return false;
			}
			if (hasDb)
			{
				try
				{
					if (resourceStoreTask.Wait(TimeSpan.FromSeconds(30)) == false)
					{
						var msg = "The database " + tenantId + " is currently being loaded, but after 30 seconds, this request has been aborted. Please try again later, database loading continues.";
						Logger.Warn(msg);
						throw new HttpException(503, msg);
					}
					if (onBeforeRequest != null)
					{
						var args = new BeforeRequestEventArgs
						{
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
					OutputDatabaseOpenFailure(tenantId, e);
					return false;
				}

				DatabaseLastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

				//TODO: check
				//if (string.IsNullOrEmpty(systemDatabase.Configuration.VirtualDirectory) == false && systemDatabase.Configuration.VirtualDirectory != "/")
				//{
				//	ctx.AdjustUrl(systemDatabase.Configuration.VirtualDirectory + match.Value);
				//}
				//else
				//{
				//	ctx.AdjustUrl(match.Value);
				//}
			}
			else
			{
				var msg = "Could open database named: " + tenantId;
				Logger.Warn(msg);
				throw new HttpException(503, msg);
			}
			return true;
		}

		private static void OutputDatabaseOpenFailure(string tenantId, Exception e)
		{
			var msg = "Could open database named: " + tenantId;
			Logger.WarnException(msg, e);
			throw new HttpException(503, msg, e);
		}

		public bool TryGetOrCreateResourceStore(string tenantId, out Task<DocumentDatabase> database)
		{
			if (ResourcesStoresCache.TryGetValue(tenantId, out database))
			{
				if (database.IsFaulted || database.IsCanceled)
				{
					ResourcesStoresCache.TryRemove(tenantId, out database);
					DateTime time;
					DatabaseLastRecentlyUsed.TryRemove(tenantId, out time);
					// and now we will try creating it again
				}
				else
				{
					return true;
				}
			}

			if (LockedDatabases.Contains(tenantId))
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");

			var config = CreateTenantConfiguration(tenantId);
			if (config == null)
				return false;

			database = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
			{
				var documentDatabase = new DocumentDatabase(config);
				AssertLicenseParameters(config);
				documentDatabase.SpinBackgroundWorkers();
				//InitializeRequestResponders(documentDatabase); TODO: don't think this is needed

				// if we have a very long init process, make sure that we reset the last idle time for this db.
				DatabaseLastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
				return documentDatabase;
			}).ContinueWith(task =>
			{
				if (task.Status == TaskStatus.Faulted) // this observes the task exception
				{
					Logger.WarnException("Failed to create database " + tenantId, task.Exception);
				}
				return task;
			}).Unwrap());
			return true;
		}

		private void AssertLicenseParameters(InMemoryRavenConfiguration config)
		{
			string maxDatabases;
			if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfDatabases", out maxDatabases))
			{
				if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
				{
					var numberOfAllowedDbs = int.Parse(maxDatabases);

					var databases = systemDatabase.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, numberOfAllowedDbs).ToList();
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

		public void ForAllDatabases(Action<DocumentDatabase> action)
		{
			action(systemDatabase);
			foreach (var value in ResourcesStoresCache
				.Select(db => db.Value)
				.Where(value => value.Status == TaskStatus.RanToCompletion))
			{
				action(value.Result);
			}
		}

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				disposed = true;
				var exceptionAggregator = new ExceptionAggregator(logger, "Could not properly dispose of HttpServer");
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
							exceptionAggregator.Execute(dbTask.Result.Dispose);
						}
						// there is no else, the db is probably faulted
					});
					ResourcesStoresCache.Clear();
				}
				exceptionAggregator.Execute(() =>
				{
					if (serverTimer != null)
						serverTimer.Dispose();
				});
			}
			finally
			{
				disposerLock.ExitWriteLock();
			}

		}

		private int reqNum;
		private int physicalRequestsCount;
		public int NumberOfRequests
		{
			get { return Thread.VolatileRead(ref physicalRequestsCount); }
		}

		public void ResetNumberOfRequests()
		{
			//TODO: implement method
			Interlocked.Exchange(ref reqNum, 0);
			Interlocked.Exchange(ref physicalRequestsCount, 0);
			//#if DEBUG
			//			while (recentRequests.Count > 0)
			//			{
			//				string _;
			//				recentRequests.TryDequeue(out _);
			//			}
			//#endif
		}

		public void IncrementRequestCount()
		{
			Interlocked.Increment(ref physicalRequestsCount);
		}

		public void DecrementRequestCount()
		{
			Interlocked.Decrement(ref physicalRequestsCount);
		}

		public void PreRequestWork(RavenApiController controller, Action action)
		{
			var isReadLockHeld = disposerLock.IsReadLockHeld;
			if (isReadLockHeld == false)
				disposerLock.EnterReadLock();
			try
			{
				if (disposed)
					return;

				if (IsWriteRequest(controller.InnerRequest))
				{
					lastWriteRequest = SystemTime.UtcNow;
				}

				var sw = Stopwatch.StartNew();
				try
				{
					action();
					//ravenUiRequest = DispatchRequest(controller);
				}
				catch (Exception e)
				{
					//TODO: handle exception
					//ExceptionHandler.TryHandleException(ctx, e);
					//if (ShouldLogException(e))
					//	logger.WarnException("Error on request", e);
				}
				finally
				{
					try
					{
						FinalizeRequestProcessing(controller, sw, ravenUiRequest: false/*TODO: check*/);
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
			}
			catch
			{
				// this can happen during system shutdown
			}
		}

		private static void AddHttpCompressionIfClientCanAcceptIt(RavenApiController controller)
		{
			var acceptEncoding = controller.GetHeader("Accept-Encoding");

			if (string.IsNullOrEmpty(acceptEncoding))
				return;

			// The Studio xap is already a compressed file, it's a waste of time to try to compress it further.
			var requestUrl = controller.GetRequestUrl();
			if (String.Equals(requestUrl, "/silverlight/Raven.Studio.xap", StringComparison.OrdinalIgnoreCase))
				return;

			// gzip must be first, because chrome has an issue accepting deflate data
			// when sending it json text
			//TODO: set these
			//if ((acceptEncoding.IndexOf("gzip", StringComparison.OrdinalIgnoreCase) != -1))
			//{
			//	ctx.SetResponseFilter(s => new GZipStream(s, CompressionMode.Compress, true));
			//	ctx.Response.AddHeader("Content-Encoding", "gzip");
			//}
			//else if (acceptEncoding.IndexOf("deflate", StringComparison.OrdinalIgnoreCase) != -1)
			//{
			//	ctx.SetResponseFilter(s => new DeflateStream(s, CompressionMode.Compress, true));
			//	ctx.Response.AddHeader("Content-Encoding", "deflate");
			//}

		}

		private Timer serverTimer;
		private bool initialized;
		private readonly DateTime startUpTime = SystemTime.UtcNow;
		private DateTime lastWriteRequest;
		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private bool disposed;

		public void Init()
		{
			if(initialized)
				return;
			initialized = true;
			TenantDatabaseModified.Occured += TenantDatabaseRemoved;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
		}

		private static bool IsWriteRequest(HttpRequestMessage request)
		{
			return AbstractRequestAuthorizer.IsGetRequest(request.Method.Method, request.RequestUri.AbsoluteUri) == false;
		}

		private void FinalizeRequestProcessing(RavenApiController controller, Stopwatch sw, bool ravenUiRequest)
		{
			LogHttpRequestStatsParams logHttpRequestStatsParam = null;
			try
			{
				logHttpRequestStatsParam = new LogHttpRequestStatsParams(
					sw,
					null, //TODO: request.Headers,
					controller.InnerRequest.Method.Method,
					200, // TODO: responseCode,
					controller.InnerRequest.RequestUri.PathAndQuery);
			}
			catch (Exception e)
			{
				logger.WarnException("Could not gather information to log request stats", e);
			}

			//ctx.FinalizeResponse();

			if (ravenUiRequest || logHttpRequestStatsParam == null || sw == null)
				return;

			sw.Stop();

			LogHttpRequestStats(logHttpRequestStatsParam, controller.DatabaseName);
			//TODO: log
			//ctx.OutputSavedLogItems(logger);
		}

		private void LogHttpRequestStats(LogHttpRequestStatsParams logHttpRequestStatsParams, string databaseName)
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
							   databaseName);
		}

		private void IdleOperations(object state)
		{
			if ((SystemTime.UtcNow - lastWriteRequest).TotalMinutes < 1)
				return;// not idle, we just had a write request coming in

			try
			{
				SystemDatabase.RunIdleOperations();
			}
			catch (Exception e)
			{
				logger.ErrorException("Error during idle operation run for system database", e);
			}

			foreach (var documentDatabase in ResourcesStoresCache)
			{
				try
				{
					if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
						continue;
					documentDatabase.Value.Result.RunIdleOperations();
				}
				catch (Exception e)
				{
					logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
				}
			}

			var databasesToCleanup = DatabaseLastRecentlyUsed
				.Where(x => (SystemTime.UtcNow - x.Value) > maxTimeDatabaseCanBeIdle)
				.Select(x => x.Key)
				.ToArray();

			foreach (var db in databasesToCleanup)
			{
				// intentionally inside the loop, so we get better concurrency overall
				// since shutting down a database can take a while
				CleanupDatabase(db, skipIfActive: true);
			}
		}
	}
}
