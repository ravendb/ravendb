using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Client.Client;
using Raven.Client.Client.Async;
using Raven.Client.Document.Async;
using System.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// Manages access to RavenDB and open sessions to work with RavenDB.
	/// </summary>
	public class DocumentStore : IDocumentStore
	{
		private static readonly Regex connectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)", 
			RegexOptions.Compiled|RegexOptions.IgnorePatternWhitespace);
		private static readonly Regex connectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
			RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private Func<IDatabaseCommands> databaseCommandsGenerator;
		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
		public NameValueCollection SharedOperationsHeaders { get; private set; }

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if (databaseCommandsGenerator == null)
                    throw new InvalidOperationException("You cannot open a session or access the database commands before initialising the document store. Did you forgot calling Initialise?");
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

			SharedOperationsHeaders = new NameValueCollection();
			Conventions = new DocumentConvention();
		}

		private string identifier;
		private IDocumentDeleteListener[] deleteListeners = new IDocumentDeleteListener[0];
		private IDocumentStoreListener[] storeListeners = new IDocumentStoreListener[0];
		private ICredentials credentials = CredentialCache.DefaultNetworkCredentials;

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
	    public string Identifier
		{
			get
			{
			    return identifier ?? Url
#if !CLIENT
			        ?? ( RunInMemory ? "memory" :  DataDirectory);
#endif
;
			}
			set { identifier = value; }
		}
#if !CLIENT
		private Raven.Database.RavenConfiguration configuration;
		
		public Raven.Database.RavenConfiguration Configuration
		{
			get
			{
				if(configuration == null)
                    configuration = new Raven.Database.RavenConfiguration();
				return configuration;
			}
			set { configuration = value; }
		}


	    /// <summary>
        /// Run RavenDB in an embedded mode, using in memory only storage.
        /// This is useful for unit tests, since it is very fast.
        /// </summary>
        public bool RunInMemory
	    {
            get { return Configuration.RunInMemory; }
	        set
	        {
	            Configuration.RunInMemory = true;
                Configuration.StorageTypeName = "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
	        }
	    }

	    /// <summary>
        /// Run RavenDB in embedded mode, using the specified directory for data storage
        /// </summary>
        /// <value>The data directory.</value>
		public string DataDirectory
		{
			get
			{
				return Configuration.DataDirectory;
			}
			set
			{
				Configuration.DataDirectory = value;
			}
		}
#endif
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
				string user = null;
				string pass = null;
				var strings = connectionStringArgumentsSplitterRegex.Split(connectionString.ConnectionString);
				foreach(var arg in strings)
				{
					var match = connectionStringRegex.Match(arg);
					if (match.Success == false)
						throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed");
					switch (match.Groups[1].Value.ToLower())
					{
#if !CLIENT
                        case "memory":
					        bool result;
                            if (bool.TryParse(match.Groups[2].Value, out result) == false)
                                throw new ConfigurationErrorsException("Could not understand memory setting: " +
                                    match.Groups[2].Value);
					        RunInMemory = result;
					        break;
						case "datadir":
							DataDirectory = match.Groups[2].Value.Trim();
							break;
#endif
                        case "resourcemanagerid":
                            ResourceManagerId = new Guid(match.Groups[2].Value.Trim());
					        break;
						case "url":
							Url = match.Groups[2].Value.Trim();
							break;

						case "user":
							user = match.Groups[2].Value.Trim();
							break;
						case "password":
								pass = match.Groups[2].Value.Trim();
							break;

						default:
							throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed, unknown option: " + match.Groups[1].Value);
					}
				}

				if (user == null && pass == null) 
					return;

				if(user == null || pass == null)
					throw new ArgumentException("User and Password must both be specified in the connection string: " + connectionStringName);
				Credentials = new NetworkCredential(user, pass);
			}
		}


		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		/// <value>The URL.</value>
		public string Url { get; set; }

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
            Stored = null;
#if !CLIENT
			if (DocumentDatabase != null)
				DocumentDatabase.Dispose();
#endif
		}

		#endregion

		/// <summary>
		/// Opens the session with the specified credentials.
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
        public IDocumentSession OpenSession(ICredentials credentialsForSession)
        {
            var session = new DocumentSession(this, storeListeners, deleteListeners, DatabaseCommands.With(credentialsForSession));
			session.Stored += OnSessionStored;
            return session;
        }

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
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		public IDocumentSession OpenSession()
        {
             var session = new DocumentSession(this, storeListeners, deleteListeners, DatabaseCommands);
			session.Stored += OnSessionStored;
            return session;
        }

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
	    public IDocumentSession OpenSession(string database)
	    {
            var session = new DocumentSession(this, storeListeners, deleteListeners, DatabaseCommands.ForDatabase(database));
            session.Stored += OnSessionStored;
            return session;
	    }

        /// <summary>
        /// Opens the session for a particular database with the specified credentials
        /// </summary>
	    public IDocumentSession OpenSession(string database, ICredentials credentialsForSession)
	    {
            var session = new DocumentSession(this, storeListeners, deleteListeners, DatabaseCommands
                .ForDatabase(database)
                .With(credentialsForSession));
            session.Stored += OnSessionStored;
            return session;
	    }

	    /// <summary>
        /// The resource manager id for the document store.
        /// IMPORTANT: Using Guid.NewGuid() to set this value is almost cetainly a mistake, you should set
        /// it to a value that remains consistent between restart of the system.
        /// </summary>
        public Guid ResourceManagerId { get; set; }

#if !CLIENT
		public Raven.Database.DocumentDatabase DocumentDatabase { get; set; }

#endif

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		public IDocumentStore Initialize()
		{
			try
			{
#if !CLIENT
				if (configuration != null)
				{
					DocumentDatabase = new Raven.Database.DocumentDatabase(configuration);
					DocumentDatabase.SpinBackgroundWorkers();
					databaseCommandsGenerator = () => new EmbededDatabaseCommands(DocumentDatabase, Conventions);
				}
				else
#endif
				{
					var replicationInformer = new ReplicationInformer();
					databaseCommandsGenerator = ()=>new ServerClient(Url, Conventions, credentials, replicationInformer);
					asyncDatabaseCommandsGenerator = ()=>new AsyncServerClient(Url, Conventions, credentials);
				}
                if(Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
                {
                    var generator = new MultiTypeHiLoKeyGenerator(this, 1024);
                    Conventions.DocumentKeyGenerator = entity => generator.GenerateDocumentKey(Conventions, entity);
                }
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
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

#if !NET_3_5

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		public IAsyncDocumentSession OpenAsyncSession()
		{
			if (DatabaseCommands == null)
				throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
			if (AsyncDatabaseCommands == null)
				throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

			var session = new AsyncDocumentSession(this, storeListeners, deleteListeners);
			session.Stored += OnSessionStored;
			return session;
		}
#endif
	}
}
