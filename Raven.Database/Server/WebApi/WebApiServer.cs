using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.SelfHost;
using Raven.Abstractions;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Plugins.Builtins.Tenants;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi.Handlers;

namespace Raven.Database.Server.WebApi
{
	public class WebApiServer : IRavenServer, IDisposable
	{
		private readonly InMemoryRavenConfiguration configuration;
		private HttpSelfHostServer server;
		private readonly DatabasesLandlord databasesLandlord;

		public DatabasesLandlord Landlord{get { return databasesLandlord; }}

		public WebApiServer(InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
		{
			this.configuration = configuration;
			databasesLandlord = new DatabasesLandlord(documentDatabase);
			mixedModeRequestAuthorizer.Initialize(documentDatabase, this);
		}

		public void SetupConfig(HttpConfiguration cfg)
		{
			cfg.Properties[typeof(DatabasesLandlord)] = databasesLandlord;
			cfg.Properties[typeof(MixedModeRequestAuthorizer)] = mixedModeRequestAuthorizer;
			cfg.Formatters.Remove(cfg.Formatters.XmlFormatter);

			cfg.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());

			cfg.MapHttpAttributeRoutes();
			cfg.Routes.MapHttpRoute(
				"API Default", "{controller}/{action}",
				new { id = RouteParameter.Optional });

			cfg.Routes.MapHttpRoute(
				"Database Route", "databases/{databaseName}/{controller}/{action}",
				new { id = RouteParameter.Optional });
			cfg.MessageHandlers.Add(new GZipToJsonHandler());
		}

		public bool HasPendingRequests
		{
			get { return false; }//TODO: fix
		}

		public void Dispose()
		{
			if (server != null)
			{
				server.CloseAsync().Wait();
				server.Dispose();
			}
		}

		public Task StartListening()
		{
			var cfg = new HttpSelfHostConfiguration(configuration.ServerUrl);
			SetupConfig(cfg);
			server = new HttpSelfHostServer(cfg);
			return server.OpenAsync();
		}
		
		public void ResetNumberOfRequests()
		{
			Landlord.ResetNumberOfRequests();
		}

		public int NumberOfRequests
		{
			get { return Landlord.NumberOfRequests; }
		}

		public static readonly Regex ChangesQuery = new Regex("^(/databases/([^/]+))?/changes/events", RegexOptions.IgnoreCase);

		public Task<DocumentDatabase> GetDatabaseInternal(string name)
		{
			return databasesLandlord.GetDatabaseInternal(name);
		}

		internal class MyAssemblyResolver : IAssembliesResolver
		{
			public ICollection<Assembly> GetAssemblies()
			{
				return new[] { typeof(RavenApiController).Assembly };
			}
		}

		public DocumentDatabase SystemDatabase
		{
			get { return databasesLandlord.SystemDatabase; }
		}

		public InMemoryRavenConfiguration SystemConfiguration
		{
			get { return databasesLandlord.SystemConfiguration; }
		}

		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private bool disposed;

		public void HandleActualRequest(IHttpContext context)
		{
			//TODO: implement
			var isReadLockHeld = disposerLock.IsReadLockHeld;
			if (isReadLockHeld == false)
				disposerLock.EnterReadLock();
			try
			{
				if (disposed)
					return;

				//if (IsWriteRequest(ctx))
				//{
				//	lastWriteRequest = SystemTime.UtcNow;
				//}
				var sw = Stopwatch.StartNew();
				bool ravenUiRequest = false;
				try
				{
					//ravenUiRequest = DispatchRequest(ctx);
				}
				catch (Exception e)
				{
					//ExceptionHandler.TryHandleException(ctx, e);
					//if (ShouldLogException(e))
					//	logger.WarnException("Error on request", e);
				}
				finally
				{
					try
					{
						//FinalizeRequestProcessing(ctx, sw, ravenUiRequest);
					}
					catch (Exception e)
					{
						//logger.ErrorException("Could not finalize request properly", e);
					}
				}
			}
			finally
			{
				if (isReadLockHeld == false)
					disposerLock.ExitReadLock();
			}
		}

		public Task HandleChangesRequest(IHttpContext httpContext, Func<bool> func)
		{
			//TODO: implement
			var sw = Stopwatch.StartNew();

			try
			{
				return new CompletedTask();
				//if (SetupRequestToProperDatabase() == false)
				//{
				//	FinalizeRequestSafe(context);
				//	onDisconnect();
				//	return new CompletedTask();
				//}

				//var eventsTransport = new EventsTransport(context);
				//eventsTransport.Disconnected += onDisconnect;
				//var handleChangesRequest = eventsTransport.ProcessAsync();
				//	return handleChangesRequest;
			}
			catch (Exception e)
			{
				//try
				//{
				//	ExceptionHandler.TryHandleException(context, e);
				//	LogException(e);
				//}
				//finally
				//{
				//	FinalizeRequestSafe(context);
				//}
				//onDisconnect();
				return new CompletedTask();
			}
			finally
			{
				//try
				//{
				//	LogHttpRequestStats(new LogHttpRequestStatsParams(
				//							sw,
				//							context.Request.Headers,
				//							context.Request.HttpMethod,
				//							context.Response.StatusCode,
				//							context.Request.Url.PathAndQuery));
				//}
				//catch (Exception e)
				//{
				//	logger.WarnException("Could not gather information to log request stats", e);
				//}
			}
		}

		private Timer serverTimer;
		private DateTime lastWriteRequest;
		private readonly TimeSpan frequencyToCheckForIdleDatabases = TimeSpan.FromMinutes(1);
		private readonly TimeSpan maxTimeDatabaseCanBeIdle;
		private MixedModeRequestAuthorizer mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();

		public void Init()
		{
			TenantDatabaseModified.Occured += databasesLandlord.TenantDatabaseRemoved;
			serverTimer = new Timer(IdleOperations, null, frequencyToCheckForIdleDatabases, frequencyToCheckForIdleDatabases);
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
				//TODO:Log
				//logger.ErrorException("Error during idle operation run for system database", e);
			}

			foreach (var documentDatabase in databasesLandlord.ResourcesStoresCache)
			{
				try
				{
					if (documentDatabase.Value.Status != TaskStatus.RanToCompletion)
						continue;
					documentDatabase.Value.Result.RunIdleOperations();
				}
				catch (Exception e)
				{
					//TODO: log
					//logger.WarnException("Error during idle operation run for " + documentDatabase.Key, e);
				}
			}

			var databasesToCleanup = databasesLandlord.DatabaseLastRecentlyUsed
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
			databasesLandlord.CleanupDatabase(db, skipIfActive);
		}
	}
}
