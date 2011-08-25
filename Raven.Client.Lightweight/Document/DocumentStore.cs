//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
#if !NET_3_5
using Raven.Client.Connection.Async;
using Raven.Client.Document.Async;
#endif
using System.Linq;
#if !SILVERLIGHT
using Raven.Client.Extensions;
using Raven.Client.Listeners;
#else
using Raven.Client.Listeners;
using Raven.Client.Silverlight.Connection;
using Raven.Client.Silverlight.Connection.Async;
#endif

namespace Raven.Client.Document
{
	/// <summary>
	/// Manages access to RavenDB and open sessions to work with RavenDB.
	/// </summary>
	public class DocumentStore : IDocumentStore
	{
		/// <summary>
		/// The current session id - only used during contsruction
		/// </summary>
		[ThreadStatic] protected static Guid? currentSessionId;

#if !SILVERLIGHT
		/// <summary>
		/// Generate new instance of database commands
		/// </summary>
		protected Func<IDatabaseCommands> databaseCommandsGenerator;
#endif
		
		private HttpJsonRequestFactory jsonRequestFactory;

		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
#if !SILVERLIGHT
	
		public System.Collections.Specialized.NameValueCollection SharedOperationsHeaders { get; private set; }
#else
		public System.Collections.Generic.IDictionary<string,string> SharedOperationsHeaders { get; private set; }
#endif

		///<summary>
		/// Get the <see cref="HttpJsonRequestFactory"/> for the stores
		///</summary>
		public HttpJsonRequestFactory JsonRequestFactory
		{
			get { return jsonRequestFactory; }
		}

#if !SILVERLIGHT
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				AssertInitialized();
				var commands = databaseCommandsGenerator();
				foreach (string key in SharedOperationsHeaders)
				{
					var values = SharedOperationsHeaders.GetValues(key);
					if(values == null)
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

#if !NET_3_5
		private Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommandsGenerator == null)
					return null;
				return asyncDatabaseCommandsGenerator();
			}
		}
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentStore"/> class.
		/// </summary>
		public DocumentStore()
		{
			ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

#if !SILVERLIGHT
			MaxNumberOfCachedRequests = 2048;
			EnlistInDistributedTransactions = true;
			SharedOperationsHeaders = new System.Collections.Specialized.NameValueCollection();
#else
			SharedOperationsHeaders = new System.Collections.Generic.Dictionary<string,string>();
#endif
			Conventions = new DocumentConvention();
		}

		private string identifier;
	    readonly DocumentSessionListeners listeners = new DocumentSessionListeners();

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
			set { credentials = value; }
		}

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
		public virtual string Identifier
		{
			get
			{
				if (identifier != null) 
					return identifier;
				if(Url == null)
					return null;
				if (DefaultDatabase != null)
					return Url + " (DB: " + DefaultDatabase + ")";
				return Url;
			}
			set { identifier = value; }
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
			if (options.ResourceManagerId != Guid.Empty)
				ResourceManagerId = options.ResourceManagerId;
			if(options.Credentials != null)
				Credentials = options.Credentials;
			if (string.IsNullOrEmpty(options.Url) == false)
				Url = options.Url;
			if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
				DefaultDatabase = options.DefaultDatabase;

			EnlistInDistributedTransactions= options.EnlistInDistributedTransactions;
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

		///<summary>
		/// Whatever or not we will automatically enlist in distributed transactions
		///</summary>
		public bool EnlistInDistributedTransactions { get; set; }
#endif


		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		public string Url { get; set; }

		/// <summary>
		/// Gets or sets the default database name.
		/// </summary>
		/// <value>The default database name.</value>
		public string DefaultDatabase { get; set; }

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public virtual void Dispose()
		{
			if (jsonRequestFactory != null) jsonRequestFactory.Dispose();
			WasDisposed = true;
			var afterDispose = AfterDispose;
			if(afterDispose!=null)
				afterDispose(this, EventArgs.Empty);
		}

		#endregion

#if !SILVERLIGHT
		/// <summary>
		/// Opens the session with the specified credentials.
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IDocumentSession OpenSession(ICredentials credentialsForSession)
		{
			EnsureNotClosed();
			
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new DocumentSession(this, listeners, sessionId, DatabaseCommands.With(credentialsForSession)
#if !NET_3_5
, AsyncDatabaseCommands.With(credentialsForSession)
#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public IDocumentSession OpenSession()
		{
			EnsureNotClosed();
			
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new DocumentSession(this, listeners, sessionId, DatabaseCommands
#if !NET_3_5
, AsyncDatabaseCommands
#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session for a particular database
		/// </summary>
		public IDocumentSession OpenSession(string database)
		{
			EnsureNotClosed();
			
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new DocumentSession(this, listeners, sessionId, DatabaseCommands.ForDatabase(database)
#if !NET_3_5
, AsyncDatabaseCommands.ForDatabase(database)
#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

		/// <summary>
		/// Opens the session for a particular database with the specified credentials
		/// </summary>
		public IDocumentSession OpenSession(string database, ICredentials credentialsForSession)
		{
			EnsureNotClosed();
			
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				var session = new DocumentSession(this, listeners, sessionId, DatabaseCommands
					.ForDatabase(database)
					.With(credentialsForSession)
#if !NET_3_5
, AsyncDatabaseCommands
					.ForDatabase(database)
					.With(credentialsForSession)
#endif
);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}

#endif
		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		public DocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			listeners.StoreListeners = listeners.StoreListeners.Concat(new[] { documentStoreListener }).ToArray();
			return this;
		}

		private void AfterSessionCreated(InMemoryDocumentSessionOperations session)
		{
			var onSessionCreatedInternal = SessionCreatedInternal;
			if(onSessionCreatedInternal!=null)
				onSessionCreatedInternal(session);
		}

		///<summary>
		/// Internal notification for integaration tools, mainly
		///</summary>
		public event Action<InMemoryDocumentSessionOperations> SessionCreatedInternal;

		/// <summary>
		/// The resource manager id for the document store.
		/// IMPORTANT: Using Guid.NewGuid() to set this value is almost certainly a mistake, you should set
		/// it to a value that remains consistent between restart of the system.
		/// </summary>
		public Guid ResourceManagerId { get; set; }

#if !NET_3_5
		
		private readonly ProfilingContext profilingContext = new ProfilingContext();
#endif

		/// <summary>
		///  Get the profiling information for the given id
		/// </summary>
		public ProfilingInformation GetProfilingInformationFor(Guid id)
		{
#if !NET_3_5
			return profilingContext.TryGet(id);
#else
			return null;
#endif
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		public  IDocumentStore Initialize()
		{
            AssertValidConfiguration();

#if !SILVERLIGHT
			jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests);
#else
			jsonRequestFactory = new HttpJsonRequestFactory();
#endif
			try
			{
#if !NET_3_5
				if(Conventions.DisableProfiling == false)
				{
					jsonRequestFactory.LogRequest += profilingContext.RecordAction;
				}
#endif
				InitializeInternal();

				InitializeSecurity();

				if(Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
				{
#if !SILVERLIGHT
					var generator = new MultiTypeHiLoKeyGenerator(this, 1024);
					Conventions.DocumentKeyGenerator = entity => generator.GenerateDocumentKey(Conventions, entity);
#else

					Conventions.DocumentKeyGenerator = entity =>
					{
						var typeTagName = Conventions.GetTypeTagName(entity.GetType());
						if (typeTagName == null)
							return Guid.NewGuid().ToString();
						return typeTagName + "/" + Guid.NewGuid();
					};
#endif
				}
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            initialized = true;

#if !SILVERLIGHT
			if(string.IsNullOrEmpty(DefaultDatabase) == false)
			{
				DatabaseCommands.GetRootDatabase().EnsureDatabaseExists(DefaultDatabase);
			}
#endif

			return this;
		}

		private void InitializeSecurity()
		{
#if !SILVERLIGHT
			string currentOauthToken = null;
			jsonRequestFactory.ConfigureRequest += (sender, args) =>
			{
				if (string.IsNullOrEmpty(currentOauthToken))
					return;
				args.Request.Headers["Authorization"] = "Bearer " + currentOauthToken;
			};
			Conventions.HandleUnauthorizedResponse = (request, response) =>
			{
				return HandleUnauthorizedResponse(request, response, ref currentOauthToken);
			};
#endif
		}

#if !SILVERLIGHT
		private bool HandleUnauthorizedResponse(HttpWebRequest request, HttpWebResponse unauthorizedResponse, ref string currentOAuthTokenValue)
		{
			var oauthSource = unauthorizedResponse.Headers["OAuth-Source"];
			if (string.IsNullOrEmpty(oauthSource))
				return false;
			var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
			authRequest.Credentials = Credentials;
			authRequest.PreAuthenticate = true;
			authRequest.Headers["grant_type"] = "client_credentials";
			authRequest.ContentType = "application/json;charset=UTF-8";
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			
			if(oauthSource.StartsWith("https", StringComparison.InvariantCultureIgnoreCase) == false && 
				jsonRequestFactory.EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers == false)
				throw new InvalidOperationException(
@"Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network.
Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism.
You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value), or setup your own behavior by providing a value for:
	documentStore.Conventions.HandleUnauthorizedResponse
If you are on an internal network or requires this for testing, you can disable this warning by calling:
	documentStore.JsonRequestFactory.EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = false;
");
				

			using(var authResponse = authRequest.GetResponse())
			using(var stream = authResponse.GetResponseStreamWithHttpDecompression())
			using(var reader = new StreamReader(stream))
			{
				currentOAuthTokenValue = reader.ReadToEnd();
				request.Headers["Authorization"] = "Bearer " + currentOAuthTokenValue;

				return true;
			}
		}
#endif

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
			var replicationInformer = new ReplicationInformer(Conventions);
			databaseCommandsGenerator = () =>
			{
				var serverClient = new ServerClient(Url, Conventions, credentials, replicationInformer, jsonRequestFactory, currentSessionId);
				if (string.IsNullOrEmpty(DefaultDatabase))
					return serverClient;
				return serverClient.ForDatabase(DefaultDatabase);
			};
#endif
#if !NET_3_5
			asyncDatabaseCommandsGenerator = () =>
			{
				var asyncServerClient = new AsyncServerClient(Url, Conventions, credentials, jsonRequestFactory, currentSessionId);
				if (string.IsNullOrEmpty(DefaultDatabase))
					return asyncServerClient;
				return asyncServerClient.ForDatabase(DefaultDatabase);
			};
#endif
		}

		/// <summary>
		/// Registers the delete listener.
		/// </summary>
		/// <param name="deleteListener">The delete listener.</param>
		/// <returns></returns>
		public DocumentStore RegisterListener(IDocumentDeleteListener deleteListener)
		{
			listeners.DeleteListeners = listeners.DeleteListeners.Concat(new[] { deleteListener }).ToArray();
			return this;
		}

		/// <summary>
		/// Registers the query listener.
		/// </summary>
		public DocumentStore RegisterListener(IDocumentQueryListener queryListener)
		{
			listeners.QueryListeners = listeners.QueryListeners.Concat(new[] { queryListener }).ToArray();
			return this;
		}
		/// <summary>
		/// Registers the convertion listener.
		/// </summary>
		public DocumentStore RegisterListener(IDocumentConversionListener conversionListener)
		{
			listeners.ConversionListeners = listeners.ConversionListeners.Concat(new[] { conversionListener, }).ToArray();
			return this;
		}


		/// <summary>
		/// Setup the context for no aggressive caching
		/// </summary>
		/// <remarks>
		/// This is mainly useful for internal use inside RavenDB, when we are executing
		/// queries that has been marked with WaitForNonStaleResults, we temporarily disable
		/// aggressive caching.
		/// </remarks>
		public IDisposable DisableAggressiveCaching()
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
		/// Setup the context for aggressive caching.
		/// </summary>
		/// <param name="cacheDuration">Specify the aggressive cache duration</param>
		/// <remarks>
		/// Aggressive caching means that we will not check the server to see whatever the response
		/// we provide is current or not, but will serve the information directly from the local cache
		/// without touching the server.
		/// </remarks>
		public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
		{
			AssertInitialized();
#if !SILVERLIGHT
			if(cacheDuration.TotalSeconds < 1)
				throw new ArgumentException("cacheDuration must be longer than a single second");

			jsonRequestFactory.AggressiveCacheDuration = cacheDuration;

			return new DisposableAction(() => jsonRequestFactory.AggressiveCacheDuration = null);
#else
			// TODO: with silverlight, we don't currently support aggressive caching
			return new DisposableAction(() => { });
#endif
		}

#if !NET_3_5
		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public IAsyncDocumentSession OpenAsyncSession()
		{
			EnsureNotClosed();
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				if (AsyncDatabaseCommands == null)
					throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

				var session = new AsyncDocumentSession(this, AsyncDatabaseCommands, listeners, sessionId);
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
		public IAsyncDocumentSession OpenAsyncSession(string databaseName)
		{
			EnsureNotClosed();
			
			var sessionId = Guid.NewGuid();
			currentSessionId = sessionId;
			try
			{
				if (AsyncDatabaseCommands == null)
					throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

				var session = new AsyncDocumentSession(this, AsyncDatabaseCommands.ForDatabase(databaseName), listeners, sessionId);
				AfterSessionCreated(session);
				return session;
			}
			finally
			{
				currentSessionId = null;
			}
		}
#endif

		private volatile EtagHolder lastEtag;
		private readonly object lastEtagLocker = new object();
		private bool initialized;

		internal void UpdateLastWrittenEtag(Guid? etag)
		{
			if (etag == null)
				return;

			var newEtag = etag.Value.ToByteArray();

			if(lastEtag == null)
			{
				lock(lastEtagLocker)
				{
					if (lastEtag == null)
					{
						lastEtag = new EtagHolder
						{
							Bytes = newEtag,
							Etag = etag.Value
						};
						return;
					}
				}
			}

			// not the most recent etag
			if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
			{
				return;
			}

			lock (lastEtagLocker)
			{
				// not the most recent etag
				if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
				{
					return;
				}

				lastEtag = new EtagHolder
				{
					Etag = etag.Value,
					Bytes = newEtag
				};
			}
		}

		///<summary>
		/// Gets the etag of the last document written by any session belonging to this 
		/// document store
		///</summary>
		public Guid? GetLastWrittenEtag()
		{
			var etagHolder = lastEtag;
			if (etagHolder == null)
				return null;
			return etagHolder.Etag;
		}

		private void EnsureNotClosed()
		{
			if (WasDisposed)
				throw new ObjectDisposedException("DocumentStore", "The document store has already been disposed and cannot be used");
		}

		private void AssertInitialized()
		{
			if (!initialized)
				throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
		}

		private class EtagHolder
		{
			public Guid Etag;
			public byte[] Bytes;
		}

		/// <summary>
		/// Called after dispose is completed
		/// </summary>
		public event EventHandler AfterDispose;

		/// <summary>
		/// Whatever the instance has been disposed
		/// </summary>
		public bool WasDisposed { get; private set; }

#if !SILVERLIGHT
		/// <summary>
		/// Max number of cached requests (default: 2048)
		/// </summary>
		public int MaxNumberOfCachedRequests { get; set; }
#endif
	}
}
