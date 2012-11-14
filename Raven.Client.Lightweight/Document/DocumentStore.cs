//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Document.OAuth;
using Raven.Client.Extensions;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Document.Async;
#if SILVERLIGHT
using System.Net.Browser;
using Raven.Client.Silverlight.Connection;
#else
using Raven.Client.Listeners;
using Raven.Client.Document.DTC;
using System.Security.Cryptography;
using System.Collections.Concurrent;
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


#if SILVERLIGHT
		private readonly Dictionary<string, ReplicationInformer> replicationInformers = new Dictionary<string, ReplicationInformer>(StringComparer.InvariantCultureIgnoreCase);
		private readonly object replicationInformersLocker = new object();
#else
		/// <summary>
		/// Generate new instance of database commands
		/// </summary>
		protected Func<IDatabaseCommands> databaseCommandsGenerator;

		private readonly ConcurrentDictionary<string, ReplicationInformer> replicationInformers = new ConcurrentDictionary<string, ReplicationInformer>(StringComparer.InvariantCultureIgnoreCase);
#endif

		private readonly AtomicDictionary<IDatabaseChanges> databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.InvariantCultureIgnoreCase);

		private HttpJsonRequestFactory jsonRequestFactory;
		private string apiKey;

		///<summary>
		/// Get the <see cref="HttpJsonRequestFactory"/> for the stores
		///</summary>
		public override HttpJsonRequestFactory JsonRequestFactory
		{
			get
			{
				AssertInitialized();
				return jsonRequestFactory;
			}
		}

#if !SILVERLIGHT
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

		private Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
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
			ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

#if !SILVERLIGHT
			MaxNumberOfCachedRequests = 2048;
			SharedOperationsHeaders = new System.Collections.Specialized.NameValueCollection();
			Conventions = new DocumentConvention();
#else
			SharedOperationsHeaders = new System.Collections.Generic.Dictionary<string,string>();
			Conventions = new DocumentConvention{AllowMultipuleAsyncOperations = true};
#endif
		}

		private string identifier;

		private bool defaultCredentials = true;
#if !SILVERLIGHT
		private ICredentials credentials = CredentialCache.DefaultNetworkCredentials;
#else
		private ICredentials credentials = new NetworkCredential();
#endif

		/// <summary>
		/// Gets or sets the credentials.
		/// </summary>
		/// <value>The credentials.</value>
		public ICredentials Credentials
		{
			get { return credentials; }
			set
			{
				credentials = value;
				defaultCredentials = false;
			}
		}

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
		public string ApiKey
		{
			get { return apiKey; }
			set
			{
				if (defaultCredentials)
					credentials = null;
				apiKey = value;
			}
		}

#if !SILVERLIGHT
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
			if (options.Credentials != null)
				Credentials = options.Credentials;
			if (string.IsNullOrEmpty(options.Url) == false)
				Url = options.Url;
			if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
				DefaultDatabase = options.DefaultDatabase;
			if (string.IsNullOrEmpty(options.ApiKey) == false)
				ApiKey = options.ApiKey;

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

#if DEBUG
		private readonly System.Diagnostics.StackTrace e = new System.Diagnostics.StackTrace();

		~DocumentStore()
		{
			var buffer = e.ToString();
			var stacktraceDebug = string.Format("StackTrace of un-disposed document store recorded. Please make sure to dispose any document store in the tests in order to avoid race conditions in tests.{0}{1}{0}{0}", Environment.NewLine, buffer);
			Console.WriteLine(stacktraceDebug);
		}
#endif

#if !SILVERLIGHT

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
				var session = new DocumentSession(this, listeners, sessionId,
					SetupCommands(DatabaseCommands, options.Database, options.Credentials, options))
					{
						DatabaseName = options.Database
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

#if !SILVERLIGHT
			jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests);
#else
			jsonRequestFactory = new HttpJsonRequestFactory();
#endif
			try
			{
				InitializeInternal();

				InitializeSecurity();

#if !SILVERLIGHT
				if (Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
				{
					var generator = new MultiTypeHiLoKeyGenerator(32);
					Conventions.DocumentKeyGenerator = (databaseCommands, entity) => generator.GenerateDocumentKey(databaseCommands, Conventions, entity);
				}
#endif

				if (Conventions.AsyncDocumentKeyGenerator == null && asyncDatabaseCommandsGenerator != null)
				{
#if !SILVERLIGHT
					var generator = new AsyncMultiTypeHiLoKeyGenerator(32);
					Conventions.AsyncDocumentKeyGenerator = (commands, entity) => generator.GenerateDocumentKeyAsync(commands, Conventions, entity);
#else
					Conventions.AsyncDocumentKeyGenerator = (commands, entity) =>
					{
						var typeTagName = Conventions.GetTypeTagName(entity.GetType());
						if (typeTagName == null)
							return CompletedTask.With(Guid.NewGuid().ToString());
						return CompletedTask.With(typeTagName + "/" + Guid.NewGuid());
					};
#endif
				}

				initialized = true;

#if !SILVERLIGHT
				RecoverPendingTransactions();

				if (string.IsNullOrEmpty(DefaultDatabase) == false)
				{
					DatabaseCommands.ForDefaultDatabase().EnsureDatabaseExists(DefaultDatabase, ignoreFailures: true);
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
			Conventions.DisableProfiling = false;
			jsonRequestFactory.LogRequest += (sender, args) =>
			{
				if (Conventions.DisableProfiling)
					return;
				profilingContext.RecordAction(sender, args);
			};
		}

#if !SILVERLIGHT
		private void RecoverPendingTransactions()
		{
			var pendingTransactionRecovery = new PendingTransactionRecovery();
			pendingTransactionRecovery.Execute(DatabaseCommands);
		}
#endif

		private void InitializeSecurity()
		{
			if (Conventions.HandleUnauthorizedResponse != null)
				return; // already setup by the user

			var basicAuthenticator = new BasicAuthenticator(credentials, ApiKey, jsonRequestFactory);
			var securedAuthenticator = new SecuredAuthenticator(ApiKey, basicAuthenticator);

			jsonRequestFactory.ConfigureRequest += basicAuthenticator.ConfigureRequest;
			jsonRequestFactory.ConfigureRequest += securedAuthenticator.ConfigureRequest;

#if !SILVERLIGHT

			Conventions.HandleUnauthorizedResponse = response =>
			{
				var oauthSource = response.Headers["OAuth-Source"];

				if (string.IsNullOrEmpty(oauthSource) == false)
				{
					return basicAuthenticator.HandleOAuthResponse(oauthSource);
				}

				if (ApiKey == null)
				{
					AssertUnuthorizedCredentialSupportWindowsAuth(response);

					return null;
				}
				oauthSource = Url + "/OAuth/API-Key";

				return securedAuthenticator.DoOAuthRequest(oauthSource);
			};

			Conventions.HandleForbiddenResponseAsync = forbiddenResponse =>
			{
				if (ApiKey == null)
				{
					AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
					return null;
				}

				return null;
			};
#endif

			Conventions.HandleUnauthorizedResponseAsync = unauthorizedResponse =>
			{
				var oauthSource = unauthorizedResponse.Headers["OAuth-Source"];

				if (string.IsNullOrEmpty(oauthSource) == false)
				{
					return basicAuthenticator.HandleOAuthResponseAsync(oauthSource);
				}

				if (ApiKey == null)
				{
					AssertUnuthorizedCredentialSupportWindowsAuth(unauthorizedResponse); 
					return null;
				}
				oauthSource = Url + "/OAuth/API-Key";

				return securedAuthenticator.DoOAuthRequestAsync(oauthSource);
			};

			Conventions.HandleForbiddenResponseAsync = forbiddenResponse =>
			{
				if (ApiKey == null)
				{
					AssertForbiddenCredentialSupportWindowsAuth(forbiddenResponse);
					return null;
				}

				return null;
			};
		}

		private void AssertUnuthorizedCredentialSupportWindowsAuth(HttpWebResponse response)
		{
			if (credentials != null)
			{
				var authHeaders = response.Headers["WWW-Authenticate"];
				if (authHeaders == null ||
					(authHeaders.Contains("NTLM") == false && authHeaders.Contains("Negotiate") == false)
					)
				{
					// we are trying to do windows auth, but we didn't get the windows auth headers
					throw new SecurityException(
						"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials," + Environment.NewLine
						+" but either worng credentials where entered or the specified server does not support Windows authentication." +
						Environment.NewLine +
						"If you are running inside IIS, make sure to enable Windows authentication.");
				}
			}
		}

		private void AssertForbiddenCredentialSupportWindowsAuth(HttpWebResponse response)
		{
			if (credentials != null)
			{
				var requiredAuth = response.Headers["Raven-Required-Auth"];
				if (requiredAuth == "Windows")
				{
					// we are trying to do windows auth, but we didn't get the windows auth headers
					throw new SecurityException(
						"Attempted to connect to a RavenDB Server that requires authentication using Windows credentials, but the specified server does not support Windows authentication." +
						Environment.NewLine +
						"If you are running inside IIS, make sure to enable Windows authentication.");
				}
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
#if !SILVERLIGHT

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
				return new ServerClient(databaseUrl, Conventions, credentials, GetReplicationInformerForDatabase, null, jsonRequestFactory, currentSessionId, listeners.ConflictListeners);
			};
#endif

			asyncDatabaseCommandsGenerator = () =>
			{
				var asyncServerClient = new AsyncServerClient(Url, Conventions, credentials, jsonRequestFactory, currentSessionId, GetReplicationInformerForDatabase, null, listeners.ConflictListeners);

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
#if SILVERLIGHT
			lock (replicationInformersLocker)
			{
				ReplicationInformer result;
				if (!replicationInformers.TryGetValue(key, out result))
				{
					result = Conventions.ReplicationInformerFactory(key);
					replicationInformers.Add(key, result);
				}
				return result;
		}
#else
			return replicationInformers.GetOrAdd(key, Conventions.ReplicationInformerFactory);
#endif
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
#if !SILVERLIGHT
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

			return databaseChanges.GetOrAdd(database ?? DefaultDatabase,
				CreateDatabaseChanges);
		}

		protected virtual IDatabaseChanges CreateDatabaseChanges(string database)
		{
			if (string.IsNullOrEmpty(Url))
				throw new InvalidOperationException("Changes API requires usage of server/client");

			database = database ?? DefaultDatabase;

			var dbUrl = MultiDatabase.GetRootDatabaseUrl(Url);
			if (string.IsNullOrEmpty(database) == false)
				dbUrl = dbUrl + "/databases/" + database;

			return new RemoteDatabaseChanges(dbUrl, credentials, jsonRequestFactory, Conventions, () => databaseChanges.Remove(database));
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
#if !SILVERLIGHT
			if (cacheDuration.TotalSeconds < 1)
				throw new ArgumentException("cacheDuration must be longer than a single second");

			var old = jsonRequestFactory.AggressiveCacheDuration;
			jsonRequestFactory.AggressiveCacheDuration = cacheDuration;

			return new DisposableAction(() => jsonRequestFactory.AggressiveCacheDuration = old);
#else
			// TODO: with silverlight, we don't currently support aggressive caching
			return new DisposableAction(() => { });
#endif
		}

		private IAsyncDocumentSession OpenAsyncSessionInternal(IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			AssertInitialized();
			EnsureNotClosed();

			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				if (AsyncDatabaseCommands == null)
					throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

				var session = new AsyncDocumentSession(this, asyncDatabaseCommands, listeners, sessionId);
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
			return OpenAsyncSessionInternal(SetupCommandsAsync(AsyncDatabaseCommands, options.Database, options.Credentials, options));
		}

		/// <summary>
		/// Called after dispose is completed
		/// </summary>
		public override event EventHandler AfterDispose;

#if !SILVERLIGHT
		/// <summary>
		/// Max number of cached requests (default: 2048)
		/// </summary>
		public int MaxNumberOfCachedRequests { get; set; }
#endif


	}
}
