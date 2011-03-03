//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Client.Client;

#if !NET_3_5
using Raven.Client.Client.Async;
using Raven.Client.Document.Async;
#endif
using System.Linq;
#if !SILVERLIGHT
using Raven.Client.Extensions;
#endif

namespace Raven.Client.Document
{
	/// <summary>
	/// Manages access to RavenDB and open sessions to work with RavenDB.
	/// </summary>
	public class DocumentStore : IDocumentStore
	{
		private static readonly Regex connectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)", 
#if !SILVERLIGHT
			RegexOptions.Compiled|
#endif
			 RegexOptions.IgnorePatternWhitespace);
		private static readonly Regex connectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
#if !SILVERLIGHT
			RegexOptions.Compiled|
#endif
					RegexOptions.IgnorePatternWhitespace);

#if !SILVERLIGHT
		/// <summary>
		/// Generate new instance of database commands
		/// </summary>
		protected Func<IDatabaseCommands> databaseCommandsGenerator;
#endif

		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
#if !SILVERLIGHT
		public System.Collections.Specialized.NameValueCollection SharedOperationsHeaders { get; private set; }
#else
		public System.Collections.Generic.IDictionary<string,string> SharedOperationsHeaders { get; private set; }
#endif

#if !SILVERLIGHT
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if (databaseCommandsGenerator == null)
					throw new InvalidOperationException("You cannot open a session or access the database commands before initialising the document store. Did you forgot calling Initialize()?");
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
		/// Occurs when an entity is stored inside any session opened from this instance
		/// </summary>
		public event EventHandler<StoredEntityEventArgs> Stored;

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentStore"/> class.
		/// </summary>
		public DocumentStore()
		{
			ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

#if !SILVERLIGHT
			SharedOperationsHeaders = new System.Collections.Specialized.NameValueCollection();
#else
			SharedOperationsHeaders = new System.Collections.Generic.Dictionary<string,string>();
#endif
			Conventions = new DocumentConvention();
		}

		private string identifier;
		private IDocumentDeleteListener[] deleteListeners = new IDocumentDeleteListener[0];
		private IDocumentStoreListener[] storeListeners = new IDocumentStoreListener[0];
		private IDocumentConversionListener[] conversionListeners = new IDocumentConversionListener[0];
		private IDocumentQueryListener[] queryListeners = new IDocumentQueryListener[0];

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
				return identifier ?? Url;
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
				var connectionString = ConfigurationManager.ConnectionStrings[connectionStringName];
				if(connectionString == null)
					throw new ArgumentException("Could not find connection string name: " + connectionStringName);
				var strings = connectionStringArgumentsSplitterRegex.Split(connectionString.ConnectionString);
				var networkCredential = new NetworkCredential();
				foreach (var arg in strings)
				{
					var match = connectionStringRegex.Match(arg);
					if (match.Success == false)
						throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed");
					ProcessConnectionStringOption(networkCredential, match.Groups[1].Value.ToLower(), match.Groups[2].Value.Trim());
				}

				if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false) 
					return;

				if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false)
					throw new ArgumentException("User and Password must both be specified in the connection string: " + connectionStringName);
				Credentials = networkCredential;
			}
		}

		private bool setupUsernameInConnectionString;
		private bool setupPasswordInConnectionString;

		/// <summary>
		/// Parse the connection string option
		/// </summary>
		protected virtual void ProcessConnectionStringOption(NetworkCredential neworkCredentials, string key, string value)
		{
			switch (key)
			{
				case "resourcemanagerid":
					ResourceManagerId = new Guid(value);
					break;
				case "url":
					Url = value;
					break;
				case "defaultdatabase":
					DefaultDatabase = value;
					break;
				case "user":
					neworkCredentials.UserName = value;
					setupUsernameInConnectionString = true;
					break;
				case "password":
					neworkCredentials.Password = value;
					setupPasswordInConnectionString = true;
					break;

				default:
					throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed, unknown option: " + key);
			}
		}
#endif


		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		/// <value>The URL.</value>
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
			Stored = null;
		}

		#endregion

#if !SILVERLIGHT
		/// <summary>
		/// Opens the session with the specified credentials.
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		public IDocumentSession OpenSession(ICredentials credentialsForSession)
		{
			var session = new DocumentSession(this, queryListeners, storeListeners, deleteListeners, DatabaseCommands.With(credentialsForSession)
#if !NET_3_5
				, AsyncDatabaseCommands.With(credentialsForSession)
#endif
			);
			AfterSessionCreated(session);
			return session;
		}

		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public IDocumentSession OpenSession()
		{
			var session = new DocumentSession(this, queryListeners, storeListeners, deleteListeners, DatabaseCommands
#if !NET_3_5
				, AsyncDatabaseCommands
#endif
);
			AfterSessionCreated(session);
			return session;
		}

		/// <summary>
		/// Opens the session for a particular database
		/// </summary>
		public IDocumentSession OpenSession(string database)
		{
			var session = new DocumentSession(this, queryListeners, storeListeners, deleteListeners, DatabaseCommands.ForDatabase(database)
#if !NET_3_5
				, AsyncDatabaseCommands.ForDatabase(database)
#endif
			);
			AfterSessionCreated(session);
			return session;
		}

		/// <summary>
		/// Opens the session for a particular database with the specified credentials
		/// </summary>
		public IDocumentSession OpenSession(string database, ICredentials credentialsForSession)
		{
			var session = new DocumentSession(this, queryListeners, storeListeners, deleteListeners, DatabaseCommands
					.ForDatabase(database)
					.With(credentialsForSession)
#if !NET_3_5
				,AsyncDatabaseCommands
					.ForDatabase(database)
					.With(credentialsForSession)
#endif
			);
			AfterSessionCreated(session); 
			return session;
		}
		
		private void AfterSessionCreated(DocumentSession session)
		{
			session.Stored += OnSessionStored;
			foreach (var documentConvertionListener in conversionListeners)
			{
				session.Advanced.OnDocumentConverted += documentConvertionListener.DocumentToEntity;
				session.Advanced.OnEntityConverted += documentConvertionListener.EntityToDocument;
			}
		}
#endif

		private void OnSessionStored(object entity)
		{
			var copy = Stored;
			if (copy != null)
				copy(this, new StoredEntityEventArgs
				{
					SessionIdentifier = Identifier, EntityInstance = entity
				});
		}

		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			storeListeners = storeListeners.Concat(new[] {documentStoreListener}).ToArray();
			return this;
		}

		

		/// <summary>
		/// The resource manager id for the document store.
		/// IMPORTANT: Using Guid.NewGuid() to set this value is almost certainly a mistake, you should set
		/// it to a value that remains consistent between restart of the system.
		/// </summary>
		public Guid ResourceManagerId { get; set; }


		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		public  IDocumentStore Initialize()
		{
			try
			{
				InitializeInternal();
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

#if !SILVERLIGHT
			if(string.IsNullOrEmpty(DefaultDatabase) == false)
			{
				DatabaseCommands.GetRootDatabase().EnsureDatabaseExists(DefaultDatabase);
			}
#endif

			return this;
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
				var serverClient = new ServerClient(Url, Conventions, credentials, replicationInformer);
				if (string.IsNullOrEmpty(DefaultDatabase))
					return serverClient;
				return serverClient.ForDatabase(DefaultDatabase);
			};
#endif
#if !NET_3_5
			asyncDatabaseCommandsGenerator = () =>
			{
				var asyncServerClient = new AsyncServerClient(Url, Conventions, credentials);
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
		public IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener)
		{
			deleteListeners = deleteListeners.Concat(new[] {deleteListener}).ToArray();
			return this;
		}

		/// <summary>
		/// Registers the query listener.
		/// </summary>
		public IDocumentStore RegisterListener(IDocumentQueryListener queryListener)
		{
			queryListeners = queryListeners.Concat(new[] { queryListener }).ToArray();
			return this;
		}
		/// <summary>
		/// Registers the convertion listener.
		/// </summary>
		public IDocumentStore RegisterListener(IDocumentConversionListener conversionListener)
		{
			conversionListeners = conversionListeners.Concat(new[] {conversionListener,}).ToArray();
			return this;
		}

#if !NET_3_5
		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public IAsyncDocumentSession OpenAsyncSession()
		{
			if (AsyncDatabaseCommands == null)
				throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

			var session = new AsyncDocumentSession(this, AsyncDatabaseCommands, queryListeners, storeListeners, deleteListeners);
			session.Stored += OnSessionStored;
			return session;
		}

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public IAsyncDocumentSession OpenAsyncSession(string databaseName)
        {
            if (AsyncDatabaseCommands == null)
                throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

            var session = new AsyncDocumentSession(this, AsyncDatabaseCommands.ForDatabase(databaseName), queryListeners, storeListeners, deleteListeners);
            session.Stored += OnSessionStored;
            return session;
        }
#endif
	}
}
