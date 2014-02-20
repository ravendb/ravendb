//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.OAuth;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Extensions;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Document.Async;
using Raven.Client.Util;

#if SILVERLIGHT
using System.Net.Browser;
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using System.Collections.Concurrent;
using Raven.Client.WinRT.Connection;
#else
using Raven.Client.Document.DTC;
#endif


namespace Raven.Client.Document
{
	/// <summary>
	/// Manages access to RavenDB and open sessions to work with RavenDB.
	/// </summary>
	public class DocumentStore : DocumentStoreBase
	{
		/// <summary>
		/// The current session id - only used during construction
		/// </summary>
		[ThreadStatic]
		protected static Guid? currentSessionId;
		private const int DefaultNumberOfCachedRequests = 2048;
		private int maxNumberOfCachedRequests = DefaultNumberOfCachedRequests;
		private bool aggressiveCachingUsed;


#if SILVERLIGHT || NETFX_CORE
		private readonly Dictionary<string, ReplicationInformer> replicationInformers = new Dictionary<string, ReplicationInformer>(StringComparer.OrdinalIgnoreCase);
		private readonly object replicationInformersLocker = new object();
#else
		/// <summary>
		/// Generate new instance of database commands
		/// </summary>
		protected Func<IDatabaseCommands> databaseCommandsGenerator;

		private readonly ConcurrentDictionary<string, ReplicationInformer> replicationInformers = new ConcurrentDictionary<string, ReplicationInformer>(StringComparer.OrdinalIgnoreCase);
#endif

		private readonly AtomicDictionary<IDatabaseChanges> databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

		private HttpJsonRequestFactory jsonRequestFactory =
#if !SILVERLIGHT && !NETFX_CORE
 new HttpJsonRequestFactory(DefaultNumberOfCachedRequests);
#else
			  new HttpJsonRequestFactory();
#endif

#if !SILVERLIGHT
		private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();
#endif

		/// <summary>
		/// Whatever this instance has json request factory available
		/// </summary>
		public override bool HasJsonRequestFactory
		{
			get { return true; }
		}

		public ReplicationBehavior Replication { get; private set; }

		///<summary>
		/// Get the <see cref="HttpJsonRequestFactory"/> for the stores
		///</summary>
		public override HttpJsonRequestFactory JsonRequestFactory
		{
			get
			{
				return jsonRequestFactory;
			}
		}

#if !SILVERLIGHT && !NETFX_CORE
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public override IDatabaseCommands DatabaseCommands
		{
			get
			{
				AssertInitialized();
				var commands = databaseCommandsGenerator();
				foreach (string key in SharedOperationsHeaders)
				{
					var values = SharedOperationsHeaders.GetValues(key);
					if (values == null)
						continue;
					foreach (var value in values)
					{
						commands.OperationsHeaders[key] = value;
					}
				}
				return commands;
			}
		}

#endif

		protected Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public override IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommandsGenerator == null)
					return null;
				return asyncDatabaseCommandsGenerator();
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentStore"/> class.
		/// </summary>
		public DocumentStore()
		{
			Replication = new ReplicationBehavior(this);
#if !SILVERLIGHT && !NETFX_CORE
			Credentials = CredentialCache.DefaultNetworkCredentials;
#endif
			ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

#if !SILVERLIGHT && !NETFX_CORE
			SharedOperationsHeaders = new System.Collections.Specialized.NameValueCollection();
			Conventions = new DocumentConvention();
#else
			SharedOperationsHeaders = new System.Collections.Generic.Dictionary<string, string>();
			Conventions = new DocumentConvention { AllowMultipuleAsyncOperations = true };
#endif
		}

		private string identifier;

#if !SILVERLIGHT
#else
		private ICredentials credentials = new NetworkCredential();
#endif

		/// <summary>
		/// Gets or sets the credentials.
		/// </summary>
		/// <value>The credentials.</value>
		public ICredentials Credentials { get; set; }

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
		public override string Identifier
		{
			get
			{
				if (identifier != null)
					return identifier;
				if (Url == null)
					return null;
				if (DefaultDatabase != null)
					return Url + " (DB: " + DefaultDatabase + ")";
				return Url;
			}
			set { identifier = value; }
		}

		/// <summary>
		/// The API Key to use when authenticating against a RavenDB server that
		/// supports API Key authentication
		/// </summary>
		public string ApiKey { get; set; }

#if !SILVERLIGHT && !NETFX_CORE
		private string connectionStringName;

		/// <summary>
		/// Gets or sets the name of the connection string name.
		/// </summary>
		public string ConnectionStringName
		{
			get { return connectionStringName; }
			set
			{
				connectionStringName = value;
				SetConnectionStringSettings(GetConnectionStringOptions());
			}
		}

		/// <summary>
		/// Set document store settings based on a given connection string.
		/// </summary>
		/// <param name="connString">The connection string to parse</param>
		public void ParseConnectionString(string connString)
		{
			var connectionStringOptions = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString(connString);
			connectionStringOptions.Parse();
			SetConnectionStringSettings(connectionStringOptions.ConnectionStringOptions);
		}

		/// <summary>
		/// Copy the relevant connection string settings
		/// </summary>
		protected virtual void SetConnectionStringSettings(RavenConnectionStringOptions options)
		{
			if (options.ResourceManagerId != Guid.Empty)
				ResourceManagerId = options.ResourceManagerId;
			if (options.Credentials != null)
				Credentials = options.Credentials;
			if (string.IsNullOrEmpty(options.Url) == false)
				Url = options.Url;
			if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
				DefaultDatabase = options.DefaultDatabase;
			if (string.IsNullOrEmpty(options.ApiKey) == false)
				ApiKey = options.ApiKey;
			if (options.FailoverServers != null)
				FailoverServers = options.FailoverServers;

			EnlistInDistributedTransactions = options.EnlistInDistributedTransactions;
		}

		/// <summary>
		/// Create the connection string parser
		/// </summary>
		protected virtual RavenConnectionStringOptions GetConnectionStringOptions()
		{
			var connectionStringOptions = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionStringName(connectionStringName);
			connectionStringOptions.Parse();
			return connectionStringOptions.ConnectionStringOptions;
		}
#endif

		/// <summary>
		/// Gets or sets the default database name.
		/// </summary>
		/// <value>The default database name.</value>
		public string DefaultDatabase { get; set; }

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public override void Dispose()
		{
#if DEBUG
			GC.SuppressFinalize(this);
#endif

#if !SILVERLIGHT
			foreach (var observeChangesAndEvictItemsFromCacheForDatabase in observeChangesAndEvictItemsFromCacheForDatabases)
			{
				observeChangesAndEvictItemsFromCacheForDatabase.Value.Dispose();
			}
#endif

			var tasks = new List<Task>();
			foreach (var databaseChange in databaseChanges)
			{
				var remoteDatabaseChanges = databaseChange.Value as RemoteDatabaseChanges;
				if (remoteDatabaseChanges != null)
				{
					tasks.Add(remoteDatabaseChanges.DisposeAsync());
				}
				else
				{
					using (databaseChange.Value as IDisposable) { }
				}
			}

			foreach (var replicationInformer in replicationInformers)
			{
				replicationInformer.Value.Dispose();
			}

			// try to wait until all the async disposables are completed
			Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(3));

			// if this is still going, we continue with disposal, it is for grace only, anyway

			if (jsonRequestFactory != null)
				jsonRequestFactory.Dispose();

			WasDisposed = true;
			var afterDispose = AfterDispose;
			if (afterDispose != null)
				afterDispose(this, EventArgs.Empty);
		}

#if DEBUG && !NETFX_CORE
		private readonly System.Diagnostics.StackTrace e = new System.Diagnostics.StackTrace();

		~DocumentStore()
		{
			var buffer = e.ToString();
			var stacktraceDebug = string.Format("StackTrace of un-disposed document store recorded. Please make sure to dispose any document store in the tests in order to avoid race conditions in tests.{0}{1}{0}{0}", Environment.NewLine, buffer);
			Console.WriteLine(stacktraceDebug);
		}
#endif

#if !SILVERLIGHT && !NETFX_CORE

		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public override IDocumentSession OpenSession()
		{
			return OpenSession(new OpenSessionOptions());
		}

		/// <summary>
		/// Opens the session for a particular database
		/// </summary>
		public override IDocumentSession OpenSession(string database)
		{
			return OpenSession(new OpenSessionOptions
			{
				Database = database
			});
		}

		public override IDocumentSession OpenSession(OpenSessionOptions options)
		{
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new DocumentSession(options.Database, this, listeners, sessionId,
					SetupCommands(DatabaseCommands, options.Database, options.Credentials, options))
					{
						DatabaseName = options.Database ?? DefaultDatabase
					};
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		private static IDatabaseCommands SetupCommands(IDatabaseCommands databaseCommands, string database, ICredentials credentialsForSession, OpenSessionOptions options)
		{
			if (database != null)
				databaseCommands = databaseCommands.ForDatabase(database);
			if (credentialsForSession != null)
				databaseCommands = databaseCommands.With(credentialsForSession);
			if (options.ForceReadFromMaster)
				databaseCommands.ForceReadFromMaster();
			return databaseCommands;
		}
#endif

		private static IAsyncDatabaseCommands SetupCommandsAsync(IAsyncDatabaseCommands databaseCommands, string database, ICredentials credentialsForSession, OpenSessionOptions options)
		{
			if (database != null)
				databaseCommands = databaseCommands.ForDatabase(database);
			if (credentialsForSession != null)
				databaseCommands = databaseCommands.With(credentialsForSession);
			if (options.ForceReadFromMaster)
				databaseCommands.ForceReadFromMaster();
			return databaseCommands;
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		public override IDocumentStore Initialize()
		{
			if (initialized)
				return this;

			AssertValidConfiguration();

#if !SILVERLIGHT && !NETFX_CORE
			jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests);
#else
			jsonRequestFactory = new HttpJsonRequestFactory();
#endif
			try
			{
				InitializeSecurity();

				InitializeInternal();

#if !SILVERLIGHT
				if (Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
				{
					var generator = new MultiDatabaseHiLoGenerator(32);
					Conventions.DocumentKeyGenerator = (dbName, databaseCommands, entity) => generator.GenerateDocumentKey(dbName, databaseCommands, Conventions, entity);
				}
#endif

				if (Conventions.AsyncDocumentKeyGenerator == null && asyncDatabaseCommandsGenerator != null)
				{
#if !SILVERLIGHT
					var generator = new AsyncMultiDatabaseHiLoKeyGenerator(32);
					Conventions.AsyncDocumentKeyGenerator = (dbName, commands, entity) => generator.GenerateDocumentKeyAsync(dbName, commands, Conventions, entity);
#else
					Conventions.AsyncDocumentKeyGenerator = (dbName, commands, entity) =>
					{
						var typeTagName = Conventions.GetTypeTagName(entity.GetType());
						if (typeTagName == null)
							return CompletedTask.With(Guid.NewGuid().ToString());
						return CompletedTask.With(typeTagName + "/" + Guid.NewGuid());
					};
#endif
				}

				initialized = true;

#if !SILVERLIGHT && !NETFX_CORE && !MONO
				RecoverPendingTransactions();

				if (string.IsNullOrEmpty(DefaultDatabase) == false && 
					DefaultDatabase.Equals(Constants.SystemDatabase) == false) //system database exists anyway
				{
					DatabaseCommands.ForSystemDatabase().GlobalAdmin.EnsureDatabaseExists(DefaultDatabase, ignoreFailures: true);
				}
#endif

			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

			return this;
		}

		public void InitializeProfiling()
		{
			if (jsonRequestFactory == null)
				throw new InvalidOperationException("Cannot call InitializeProfiling() before Initialize() was called.");
			Conventions.DisableProfiling = false;
			jsonRequestFactory.LogRequest += (sender, args) =>
			{
				if (Conventions.DisableProfiling)
					return;
				if (args.TotalSize > 1024 * 1024 * 2)
				{
					profilingContext.RecordAction(sender, new RequestResultArgs
					{
						Url = args.Url,
						PostedData = "total request/response size > 2MB, not tracked",
						Result = "total request/response size > 2MB, not tracked",
					});
					return;
				}
				profilingContext.RecordAction(sender, args);
			};
		}

#if !SILVERLIGHT && !NETFX_CORE
		private void RecoverPendingTransactions()
		{
			if (EnlistInDistributedTransactions == false)
				return;

			var pendingTransactionRecovery = new PendingTransactionRecovery(this);
			pendingTransactionRecovery.Execute(ResourceManagerId, DatabaseCommands);
		}
#endif

		private void InitializeSecurity()
		{
			if (Conventions.HandleUnauthorizedResponseAsync != null)
				return; // already setup by the user

			if (string.IsNullOrEmpty(ApiKey) == false)
			{
				Credentials = null;
			}

			var basicAuthenticator = new BasicAuthenticator(jsonRequestFactory.EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers);
			var securedAuthenticator = new SecuredAuthenticator();

			jsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
			jsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

			Conventions.HandleForbiddenResponseAsync = (forbiddenResponse, credentials) =>
			{
				if (credentials.ApiKey == null)
				{
					AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
					return null;
				}

				return null;
			};

			Conventions.HandleUnauthorizedResponseAsync = (unauthorizedResponse, credentials) =>
			{
				var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

#if DEBUG && FIDDLER
                // Make sure to avoid a cross DNS security issue, when running with Fiddler
				if (string.IsNullOrEmpty(oauthSource) == false)
					oauthSource = oauthSource.Replace("localhost:", "localhost.fiddler:");
#endif

				// Legacy support
				if (string.IsNullOrEmpty(oauthSource) == false &&
					oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false)
				{
					return basicAuthenticator.HandleOAuthResponseAsync(oauthSource, credentials.ApiKey);
				}

				if (credentials.ApiKey == null)
				{
					AssertUnauthorizedCredentialSupportWindowsAuth(unauthorizedResponse, credentials.Credentials);
					return null;
				}

				if (string.IsNullOrEmpty(oauthSource))
					oauthSource = Url + "/OAuth/API-Key";

				return securedAuthenticator.DoOAuthRequestAsync(Url, oauthSource, credentials.ApiKey);
			};

		}

		private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpWebResponse response, ICredentials credentials)
		{
			if (credentials == null)
				return;

			var authHeaders = response.Headers["WWW-Authenticate"];
			if (authHeaders == null ||
				(authHeaders.Contains("NTLM") == false && authHeaders.Contains("Negotiate") == false)
				)
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
					+ " but either wrong credentials where entered or the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response, ICredentials credentials)
		{
			if (credentials == null)
				return;

			var authHeaders = response.Headers.WwwAuthenticate.FirstOrDefault();
			if (authHeaders == null ||
				(authHeaders.ToString().Contains("NTLM") == false && authHeaders.ToString().Contains("Negotiate") == false)
				)
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
					+ " but either wrong credentials where entered or the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		private void AssertUnauthorizedCredentialSupportWindowsAuth(HttpResponseMessage response)
		{
			if (Credentials == null)
				return;

			var authHeaders = response.Headers.GetFirstValue("WWW-Authenticate");
			if (authHeaders == null ||
				(authHeaders.Contains("NTLM") == false && authHeaders.Contains("Negotiate") == false)
				)
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
					+ " but either wrong credentials where entered or the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		private void AssertForbiddenCredentialSupportWindowsAuth(HttpResponseMessage response)
		{
			if (Credentials == null)
				return;

			var requiredAuth = response.Headers.GetFirstValue("Raven-Required-Auth");
			if (requiredAuth == "Windows")
			{
				// we are trying to do windows auth, but we didn't get the windows auth headers
				throw new SecurityException(
					"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials, but the specified server does not support Windows authentication." +
					Environment.NewLine +
					"If you are running inside IIS, make sure to enable Windows authentication.");
			}
		}

		/// <summary>
		/// validate the configuration for the document store
		/// </summary>
		protected virtual void AssertValidConfiguration()
		{
			if (string.IsNullOrEmpty(Url))
				throw new ArgumentException("Document store URL cannot be empty", "Url");
		}

		/// <summary>
		/// Initialize the document store access method to RavenDB
		/// </summary>
		protected virtual void InitializeInternal()
		{
#if !SILVERLIGHT && !NETFX_CORE

			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);
			var rootServicePoint = ServicePointManager.FindServicePoint(new Uri(rootDatabaseUrl));
			rootServicePoint.UseNagleAlgorithm = false;
			rootServicePoint.Expect100Continue = false;
			rootServicePoint.ConnectionLimit = 256;

			databaseCommandsGenerator = () =>
			{
				string databaseUrl = Url;
				if (string.IsNullOrEmpty(DefaultDatabase) == false)
				{
					databaseUrl = rootDatabaseUrl;
					databaseUrl = databaseUrl + "/databases/" + DefaultDatabase;
				}
				return new ServerClient(new AsyncServerClient(databaseUrl, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory,
					currentSessionId, GetReplicationInformerForDatabase, null,
					listeners.ConflictListeners));
			};
#endif

#if SILVERLIGHT
			WebRequest.RegisterPrefix("http://", WebRequestCreator.ClientHttp);
			WebRequest.RegisterPrefix("https://", WebRequestCreator.ClientHttp);
			
			// required to ensure just a single auth dialog
			var task = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, (Url + "/docs?pageSize=0").NoCache(), "GET", new OperationCredentials(ApiKey, Credentials), Conventions))
				.ExecuteRequestAsync();
			
			jsonRequestFactory.ConfigureRequest += (sender, args) =>
			{
				args.JsonRequest.WaitForTask = task;
			};
#endif

			asyncDatabaseCommandsGenerator = () =>
			{
				var asyncServerClient = new AsyncServerClient(Url, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory, currentSessionId, GetReplicationInformerForDatabase, null, listeners.ConflictListeners);

				if (string.IsNullOrEmpty(DefaultDatabase))
					return asyncServerClient;
				return asyncServerClient.ForDatabase(DefaultDatabase);
			};
		}


		public ReplicationInformer GetReplicationInformerForDatabase(string dbName = null)
		{
			var key = Url;
			dbName = dbName ?? DefaultDatabase;
			if (string.IsNullOrEmpty(dbName) == false)
			{
				key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + dbName;
			}
			ReplicationInformer result;

#if SILVERLIGHT || NETFX_CORE
			lock (replicationInformersLocker)
			{
				if (!replicationInformers.TryGetValue(key, out result))
				{
					result = Conventions.ReplicationInformerFactory(key);
					replicationInformers.Add(key, result);
				}
			}
#else
			result = replicationInformers.GetOrAdd(key, Conventions.ReplicationInformerFactory);

#endif

			if (FailoverServers == null)
				return result;

			if (dbName == DefaultDatabase)
			{
				if (FailoverServers.IsSetForDefaultDatabase && result.FailoverServers == null)
					result.FailoverServers = FailoverServers.ForDefaultDatabase;
		}
			else
			{
				if (FailoverServers.IsSetForDatabase(dbName) && result.FailoverServers == null)
					result.FailoverServers = FailoverServers.GetForDatabase(dbName);
			}

			return result;
		}

		/// <summary>
		/// Setup the context for no aggressive caching
		/// </summary>
		/// <remarks>
		/// This is mainly useful for internal use inside RavenDB, when we are executing
		/// queries that have been marked with WaitForNonStaleResults, we temporarily disable
		/// aggressive caching.
		/// </remarks>
		public override IDisposable DisableAggressiveCaching()
		{
			AssertInitialized();
#if !SILVERLIGHT && !NETFX_CORE
			var old = jsonRequestFactory.AggressiveCacheDuration;
			jsonRequestFactory.AggressiveCacheDuration = null;
			return new DisposableAction(() => jsonRequestFactory.AggressiveCacheDuration = old);
#else
			// TODO: with silverlight, we don't currently support aggressive caching
			return new DisposableAction(() => { });
#endif
		}

		/// <summary>
		/// Subscribe to change notifications from the server
		/// </summary>
		public override IDatabaseChanges Changes(string database = null)
		{
			AssertInitialized();

			return databaseChanges.GetOrAdd(database ?? DefaultDatabase, CreateDatabaseChanges);
		}

		protected virtual IDatabaseChanges CreateDatabaseChanges(string database)
		{
			if (string.IsNullOrEmpty(Url))
				throw new InvalidOperationException("Changes API requires usage of server/client");

			database = database ?? DefaultDatabase;

			var dbUrl = MultiDatabase.GetRootDatabaseUrl(Url);
			if (string.IsNullOrEmpty(database) == false)
				dbUrl = dbUrl + "/databases/" + database;

			using(NoSynchronizationContext.Scope())
			{
			return new RemoteDatabaseChanges(dbUrl,
					ApiKey,
				Credentials,
				jsonRequestFactory,
				Conventions,
				GetReplicationInformerForDatabase(database),
				() => databaseChanges.Remove(database),
					((AsyncServerClient) AsyncDatabaseCommands).TryResolveConflictByUsingRegisteredListenersAsync);
			}
		}

		/// <summary>
		/// Setup the context for aggressive caching.
		/// </summary>
		/// <param name="cacheDuration">Specify the aggressive cache duration</param>
		/// <remarks>
		/// Aggressive caching means that we will not check the server to see whatever the response
		/// we provide is current or not, but will serve the information directly from the local cache
		/// without touching the server.
		/// </remarks>
		public override IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
		{
			AssertInitialized();
#if !SILVERLIGHT && !NETFX_CORE
			if (cacheDuration.TotalSeconds < 1)
				throw new ArgumentException("cacheDuration must be longer than a single second");

			var old = jsonRequestFactory.AggressiveCacheDuration;
			jsonRequestFactory.AggressiveCacheDuration = cacheDuration;

			aggressiveCachingUsed = true;

			return new DisposableAction(() =>
			{
				jsonRequestFactory.AggressiveCacheDuration = old;
			});
#else
			// TODO: with silverlight, we don't currently support aggressive caching
			return new DisposableAction(() => { });
#endif
		}

		/// <summary>
		/// Setup the WebRequest timeout for the session
		/// </summary>
		/// <param name="timeout">Specify the timeout duration</param>
		/// <remarks>
		/// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
		/// </remarks>
		public override IDisposable SetRequestsTimeoutFor(TimeSpan timeout) {
			AssertInitialized();
#if !SILVERLIGHT && !NETFX_CORE

			var old = jsonRequestFactory.RequestTimeout;
			jsonRequestFactory.RequestTimeout = timeout;

			return new DisposableAction(() => {
				jsonRequestFactory.RequestTimeout = old;
			});
#else
			// TODO: with silverlight, we don't currently support session timeout
			return new DisposableAction(() => { });
#endif
		}

		private IAsyncDocumentSession OpenAsyncSessionInternal(string dbName, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			AssertInitialized();
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				if (AsyncDatabaseCommands == null)
					throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

				var session = new AsyncDocumentSession(dbName, this, asyncDatabaseCommands, listeners, sessionId)
				{
				    DatabaseName = dbName ?? DefaultDatabase
				};
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public override IAsyncDocumentSession OpenAsyncSession()
		{
			return OpenAsyncSession(new OpenSessionOptions());
		}

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public override IAsyncDocumentSession OpenAsyncSession(string databaseName)
		{
			return OpenAsyncSession(new OpenSessionOptions
			{
				Database = databaseName
			});
		}

		public IAsyncDocumentSession OpenAsyncSession(OpenSessionOptions options)
		{
            return OpenAsyncSessionInternal(options.Database, SetupCommandsAsync(AsyncDatabaseCommands, options.Database, options.Credentials, options));
		}

		/// <summary>
		/// Called after dispose is completed
		/// </summary>
		public override event EventHandler AfterDispose;

#if !SILVERLIGHT && !NETFX_CORE
		/// <summary>
		/// Max number of cached requests (default: 2048)
		/// </summary>
		public int MaxNumberOfCachedRequests
		{
			get { return maxNumberOfCachedRequests; }
			set
			{
				maxNumberOfCachedRequests = value;
				if (jsonRequestFactory != null)
					jsonRequestFactory.Dispose();
				jsonRequestFactory = new HttpJsonRequestFactory(maxNumberOfCachedRequests);
			}
		}
#endif


#if !SILVERLIGHT && !NETFX_CORE
		public override BulkInsertOperation BulkInsert(string database = null, BulkInsertOptions options = null)
		{
			return new BulkInsertOperation(database ?? DefaultDatabase, this, listeners, options ?? new BulkInsertOptions(), Changes(database ?? DefaultDatabase));
		}

		protected override void AfterSessionCreated(InMemoryDocumentSessionOperations session)
		{
			if (Conventions.ShouldAggressiveCacheTrackChanges && aggressiveCachingUsed)
			{
				var databaseName = session.DatabaseName;
				observeChangesAndEvictItemsFromCacheForDatabases.GetOrAdd(databaseName ?? Constants.SystemDatabase,
					_ => new EvictItemsFromCacheBasedOnChanges(databaseName ?? Constants.SystemDatabase,
						Changes(databaseName),
						jsonRequestFactory.ExpireItemsFromCache));
			}

			base.AfterSessionCreated(session);
		}


		public Task GetObserveChangesAndEvictItemsFromCacheTask(string database = null)
		{
			var changes = observeChangesAndEvictItemsFromCacheForDatabases.GetOrDefault(database ?? Constants.SystemDatabase);
			return changes == null ? new CompletedTask() : changes.ConnectionTask;
		}
#endif
	}
}
