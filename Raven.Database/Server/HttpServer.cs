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
using Raven.Abstractions.Util;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Responders;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Plugins.Builtins.Tenants;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Security.OAuth;
using Raven.Database.Server.Security.Windows;

namespace Raven.Database.Server
{
	public class HttpServer : IDisposable
	{
		private readonly DateTime startUpTime = SystemTime.UtcNow;
		private DateTime lastWriteRequest;
		private const int MaxConcurrentRequests = 192;
		public DocumentDatabase SystemDatabase { get; private set; }
		public InMemoryRavenConfiguration SystemConfiguration { get; private set; }
		readonly AbstractRequestAuthorizer requestAuthorizer;

		public event Action<InMemoryRavenConfiguration> SetupTenantDatabaseConfiguration = delegate { };

		private readonly ThreadLocal<string> currentTenantId = new ThreadLocal<string>();
		private readonly ThreadLocal<DocumentDatabase> currentDatabase = new ThreadLocal<DocumentDatabase>();
		private readonly ThreadLocal<InMemoryRavenConfiguration> currentConfiguration = new ThreadLocal<InMemoryRavenConfiguration>();

		protected readonly ConcurrentSet<string> LockedDatabases =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		protected readonly ConcurrentDictionary<string, DocumentDatabase> ResourcesStoresCache =
			new ConcurrentDictionary<string, DocumentDatabase>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentDictionary<string, DateTime> databaseLastRecentlyUsed = new ConcurrentDictionary<string, DateTime>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();

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

		private static readonly Logger logger = LogManager.GetCurrentClassLogger();

		private int reqNum;


		// concurrent requests
		// we set 1/4 aside for handling background tasks
		private readonly SemaphoreSlim concurretRequestSemaphore = new SemaphoreSlim(MaxConcurrentRequests);
		private Timer serverTimer;
		private int physicalRequestsCount;

		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private readonly TimeSpan frequnecyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private bool disposed;

		public bool HasPendingRequests
		{
			get { return concurretRequestSemaphore.CurrentCount != MaxConcurrentRequests; }
		}

		public HttpServer(InMemoryRavenConfiguration configuration, DocumentDatabase resourceStore)
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();

			SystemDatabase = resourceStore;
			SystemConfiguration = configuration;

			int val;
			if (int.TryParse(configuration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
				val = 900;
			maxTimeDatabaseCanBeIdle = TimeSpan.FromSeconds(val);
			if (int.TryParse(configuration.Settings["Raven/Tenants/FrequnecyToCheckForIdleDatabases"], out val) == false)
				val = 60;
			frequnecyToCheckForIdleDatabases = TimeSpan.FromSeconds(val);

			configuration.Container.SatisfyImportsOnce(this);

			InitializeRequestResponders(SystemDatabase);

			switch (configuration.AuthenticationMode.ToLowerInvariant())
			{
				case "windows":
					requestAuthorizer = new WindowsRequestAuthorizer();
					break;
				case "oauth":
					requestAuthorizer = new OAuthRequestAuthorizer();
					break;
				default:
					throw new InvalidOperationException(
						string.Format("Unknown AuthenticationMode {0}. Options are Windows and OAuth", configuration.AuthenticationMode));
			}

			requestAuthorizer.Initialize(() => currentDatabase.Value, () => currentConfiguration.Value, () => currentTenantId.Value, this);
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

			CleanupDatabase(@event.Name, skipIfActive: false);
		}

		public object Statistics
		{
			get
			{
				return new
				{
					TotalNumberOfRequests = NumberOfRequests,
					Uptime = SystemTime.UtcNow - startUpTime,
					LoadedDatabases =
						from documentDatabase in ResourcesStoresCache
								.Concat(new[] { new KeyValuePair<string, DocumentDatabase>("System", SystemDatabase), })
						let totalSizeOnDisk = documentDatabase.Value.GetTotalSizeOnDisk()
						let lastUsed = databaseLastRecentlyUsed.GetOrDefault(documentDatabase.Key)
						select new
						{
							Name = documentDatabase.Key,
							LastActivity = new[]
							{
								lastUsed, 
								documentDatabase.Value.WorkContext.LastWorkTime,
							}.Max(),
							Size = totalSizeOnDisk,
							HumaneSize = DatabaseSize.Humane(totalSizeOnDisk),
							documentDatabase.Value.Statistics.CountOfDocuments,
						}
				};
			}
		}

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				TenantDatabaseModified.Occured -= TenantDatabaseRemoved;
				var exceptionAggregator = new ExceptionAggregator(logger, "Could not properly dispose of HttpServer");
				exceptionAggregator.Execute(() =>
				{
					if (serverTimer != null)
						serverTimer.Dispose();
				});
				exceptionAggregator.Execute(() =>
				{
					if (listener != null && listener.IsListening)
						listener.Stop();
				});
				disposed = true;

				exceptionAggregator.Execute(() =>
				{
					lock (ResourcesStoresCache)
					{
						// shut down all databases in parallel, avoid having to wait for each one
						Parallel.ForEach(ResourcesStoresCache, val => val.Value.Dispose());
						ResourcesStoresCache.Clear();
					}
				});

				exceptionAggregator.Execute(currentConfiguration.Dispose);
				exceptionAggregator.Execute(currentDatabase.Dispose);
				exceptionAggregator.Execute(currentTenantId.Dispose);

				exceptionAggregator.ThrowIfNeeded();
			}
			finally
			{
				disposerLock.ExitWriteLock();
			}
		}

		public void StartListening()
		{
			listener = new HttpListener();
			string virtualDirectory = SystemConfiguration.VirtualDirectory;
			if (virtualDirectory.EndsWith("/") == false)
				virtualDirectory = virtualDirectory + "/";
			var uri = "http://" + (SystemConfiguration.HostName ?? "+") + ":" + SystemConfiguration.Port + virtualDirectory;
			listener.Prefixes.Add(uri);

			foreach (var configureHttpListener in ConfigureHttpListeners)
			{
				configureHttpListener.Value.Configure(listener, SystemConfiguration);
			}

			Init();
			listener.Start();


			listener.BeginGetContext(GetContext, null);
		}

		public void Init()
		{
			TenantDatabaseModified.Occured += TenantDatabaseRemoved;
			serverTimer = new Timer(IdleOperations, null, frequnecyToCheckForIdleDatabases, frequnecyToCheckForIdleDatabases);
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
					documentDatabase.Value.RunIdleOperations();
				}
				catch (Exception e)
				{
					logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
				}
			}

			var databasesToCleanup = databaseLastRecentlyUsed
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

		protected void CleanupDatabase(string db, bool skipIfActive)
		{
			lock (ResourcesStoresCache)
			{
				DateTime time;
				DocumentDatabase database;
				if (ResourcesStoresCache.TryGetValue(db, out database) == false)
				{
					databaseLastRecentlyUsed.TryRemove(db, out time);
					return;
				}
				if (skipIfActive && (SystemTime.UtcNow - database.WorkContext.LastWorkTime).TotalMinutes < 5)
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
					logger.ErrorException("Could not cleanup tenant database: " + db, e);
					return;
				}
				databaseLastRecentlyUsed.TryRemove(db, out time);
				ResourcesStoresCache.TryRemove(db, out database);
			}
		}

		private void GetContext(IAsyncResult ar)
		{
			IHttpContext ctx;
			try
			{
				HttpListenerContext httpListenerContext = listener.EndGetContext(ar);
				ctx = new HttpListenerContextAdpater(httpListenerContext, SystemConfiguration);
				//setup waiting for the next request
				listener.BeginGetContext(GetContext, null);
			}
			catch (Exception)
			{
				// can't get current request / end new one, probably
				// listener shutdown
				return;
			}

			if (concurretRequestSemaphore.Wait(TimeSpan.FromSeconds(5)) == false)
			{
				HandleTooBusyError(ctx);
				return;
			}
			try
			{
				Interlocked.Increment(ref physicalRequestsCount);
				if (ChangesQuery.IsMatch(ctx.GetRequestUrl()))
					HandleChangesRequest(ctx, () => { });
				else
					HandleActualRequest(ctx);
			}
			finally
			{
				concurretRequestSemaphore.Release();
			}
		}


		public Task HandleChangesRequest(IHttpContext context, Action onDisconnect)
		{
			var sw = Stopwatch.StartNew();
			try
			{
				SetupRequestToProperDatabase(context);
				if (!SetThreadLocalState(context))
				{
					context.FinalizeResonse();
					onDisconnect();
					return new CompletedTask();
				}
				var eventsTransport = new EventsTransport(context);
				eventsTransport.Disconnected += onDisconnect;
				CurrentDatabase.TransportState.Register(eventsTransport);
				return eventsTransport.ProcessAsync();
			}
			catch (Exception e)
			{
				try
				{
					HandleException(context, e);
					if (ShouldLogException(e))
						logger.WarnException("Error on request", e);
				}
				finally
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

		public void HandleActualRequest(IHttpContext ctx)
		{
			var isReadLockHeld = disposerLock.IsReadLockHeld;
			if (isReadLockHeld == false)
				disposerLock.EnterReadLock();
			try
			{
				if (disposed)
					return;

				if (IsWriteRequest(ctx))
				{
					lastWriteRequest = SystemTime.UtcNow;
				}
				var sw = Stopwatch.StartNew();
				bool ravenUiRequest = false;
				try
				{
					ravenUiRequest = DispatchRequest(ctx);
				}
				catch (Exception e)
				{
					HandleException(ctx, e);
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

			ctx.FinalizeResonse();

			if (ravenUiRequest || logHttpRequestStatsParam == null)
				return;

			sw.Stop();

			LogHttpRequestStats(logHttpRequestStatsParam);
			ctx.OutputSavedLogItems(logger);
		}

		private void LogHttpRequestStats(LogHttpRequestStatsParams logHttpRequestStatsParams)
		{
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

		private void HandleException(IHttpContext ctx, Exception e)
		{
			try
			{
				if (e is BadRequestException)
					HandleBadRequest(ctx, (BadRequestException)e);
				else if (e is ConcurrencyException)
					HandleConcurrencyException(ctx, (ConcurrencyException)e);
				else if (TryHandleException(ctx, e))
					return;
				else
					HandleGenericException(ctx, e);
			}
			catch (Exception)
			{
				logger.ErrorException("Failed to properly handle error, further error handling is ignored", e);
			}
		}

		protected bool TryHandleException(IHttpContext ctx, Exception exception)
		{
			var indexDisabledException = exception as IndexDisabledException;
			if (indexDisabledException != null)
			{
				HandleIndexDisabledException(ctx, indexDisabledException);
				return true;
			}
			var indexDoesNotExistsException = exception as IndexDoesNotExistsException;
			if (indexDoesNotExistsException != null)
			{
				HandleIndexDoesNotExistsException(ctx, indexDoesNotExistsException);
				return true;
			}

			return false;
		}

		private static void HandleIndexDoesNotExistsException(IHttpContext ctx, Exception e)
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

		private static void HandleTooBusyError(IHttpContext ctx)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = "The server is too busy, could not acquire transactional access"
			});
		}


		private static void HandleGenericException(IHttpContext ctx, Exception e)
		{
			ctx.Response.StatusCode = 500;
			ctx.Response.StatusDescription = "Internal Server Error";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = e.ToString()
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
				e.ActualETag,
				e.ExpectedETag,
				Error = e.Message
			});
		}

		protected static void SerializeError(IHttpContext ctx, object error)
		{
			var sw = new StreamWriter(ctx.Response.OutputStream);
			JsonExtensions.CreateDefaultJsonSerializer().Serialize(new JsonTextWriter(sw)
			{
				Formatting = Formatting.Indented,
			}, error);
			sw.Flush();
		}

		private bool DispatchRequest(IHttpContext ctx)
		{
			Action onResponseEnd = null;

			SetupRequestToProperDatabase(ctx);
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
						requestResponder.Respond(ctx);
						sp.Stop();
						ctx.Response.AddHeader("Temp-Request-Time", sp.ElapsedMilliseconds.ToString("#,# ms", CultureInfo.InvariantCulture));
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
		}

		public DocumentDatabase CurrentDatabase
		{
			get { return currentDatabase.Value ?? SystemDatabase; }
		}

		private void SetupRequestToProperDatabase(IHttpContext ctx)
		{
			var requestUrl = ctx.GetRequestUrlForTenantSelection();
			var match = databaseQuery.Match(requestUrl);

			if (match.Success == false)
			{
				currentTenantId.Value = Constants.SystemDatabase;
				currentDatabase.Value = SystemDatabase;
				currentConfiguration.Value = SystemConfiguration;
				databaseLastRecentlyUsed.AddOrUpdate("System", SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);
				return;
			}
			var tenantId = match.Groups[1].Value;
			DocumentDatabase resourceStore;
			if (TryGetOrCreateResourceStore(tenantId, out resourceStore))
			{
				databaseLastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

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
				throw new BadRequestException("Could not find a database named: " + tenantId);
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

		public void ForAllDatabases(Action<DocumentDatabase> action)
		{
			action(SystemDatabase);
			foreach (var db in ResourcesStoresCache)
			{
				action(db.Value);
			}
		}

		protected bool TryGetOrCreateResourceStore(string tenantId, out DocumentDatabase database)
		{
			if (ResourcesStoresCache.TryGetValue(tenantId, out database))
				return true;

			if (LockedDatabases.Contains(tenantId))
				throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed");

			var config = CreateTenantConfiguration(tenantId);
			if (config == null)
				return false;

			database = ResourcesStoresCache.GetOrAddAtomically(tenantId, s =>
			{
				var documentDatabase = new DocumentDatabase(config);
				documentDatabase.SpinBackgroundWorkers();
				InitializeRequestResponders(documentDatabase);
				return documentDatabase;
			});
			return true;
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

			return document;
		}


		private void AddAccessControlHeaders(IHttpContext ctx)
		{
			if (string.IsNullOrEmpty(SystemConfiguration.AccessControlAllowOrigin))
				return;
			ctx.Response.AddHeader("Access-Control-Allow-Origin", SystemConfiguration.AccessControlAllowOrigin);
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
			if (String.Equals(requestUrl, "/silverlight/Raven.Studio.xap", StringComparison.InvariantCultureIgnoreCase))
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
		}

		public DocumentDatabase GetDatabase(string name)
		{
			if (string.Equals("System", name, StringComparison.InvariantCultureIgnoreCase))
				return SystemDatabase;

			DocumentDatabase db;
			if (TryGetOrCreateResourceStore(name, out db))
				return db;

			throw new BadRequestException("Could not find a database named: " + name);
		}

		public void Protect(DatabaseDocument databaseDocument)
		{
			if (databaseDocument.SecuredSettings == null)
			{
				databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
				return;
			}

			foreach (var prop in databaseDocument.SecuredSettings.ToList())
			{
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
				databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
				return;
			}

			foreach (var prop in databaseDocument.SecuredSettings.ToList())
			{
				var bytes = Convert.FromBase64String(prop.Value);
				var entrophy = Encoding.UTF8.GetBytes(prop.Key);
				var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
				databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
			}
		}
	}
}
