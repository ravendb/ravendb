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

        /// <summary>
        /// Generate new instance of database commands
        /// </summary>
	    protected Func<IDatabaseCommands> databaseCommandsGenerator;
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
	    public virtual string Identifier
		{
			get
			{
			    return identifier ?? Url;
			}
			set { identifier = value; }
		}
	
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

                if (networkCredential.UserName == null && networkCredential.Password == null) 
					return;

                if (networkCredential.UserName == null || networkCredential.Password== null)
					throw new ArgumentException("User and Password must both be specified in the connection string: " + connectionStringName);
			    Credentials = networkCredential;
			}
		}

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

	            case "user":
	                neworkCredentials.UserName = value;
	                break;
	            case "password":
                    neworkCredentials.Password = value;
	                break;

	            default:
                    throw new ArgumentException("Connection string name: " + connectionStringName + " could not be parsed, unknown option: " + key);
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
		public virtual void Dispose()
		{
            Stored = null;
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
        /// Initialize the document store access method to RavenDB
        /// </summary>
	    protected virtual void InitializeInternal()
	    {
	        var replicationInformer = new ReplicationInformer();
	        databaseCommandsGenerator = () => new ServerClient(Url, Conventions, credentials, replicationInformer);
	        asyncDatabaseCommandsGenerator = () => new AsyncServerClient(Url, Conventions, credentials);
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
