using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Client.Client;
using Raven.Client.Client.Async;
using Raven.Client.Document.Async;
using System.Linq;

namespace Raven.Client.Document
{
	public class DocumentStore : IDocumentStore
	{
		private static readonly Regex connectionStringRegex = new Regex(@"(\w+) \s* = \s* (.*)", 
			RegexOptions.Compiled|RegexOptions.IgnorePatternWhitespace);
		private static readonly Regex connectionStringArgumentsSplitterRegex = new Regex(@"; (?=\s* \w+ \s* =)",
			RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private Func<IDatabaseCommands> databaseCommandsGenerator;
		private IDatabaseCommands databaseCommands;

		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if (databaseCommandsGenerator == null)
					return null;

				if (databaseCommands == null)
					databaseCommands = databaseCommandsGenerator();

				return databaseCommands;
			}
		}

		private Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommandsGenerator == null)
					return null;
				return asyncDatabaseCommandsGenerator();
			}
		}

		public event EventHandler<StoredEntityEventArgs> Stored;

		public DocumentStore()
		{
			Conventions = new DocumentConvention();
		}

		private string identifier;
		private IDocumentDeleteListener[] deleteListeners = new IDocumentDeleteListener[0];
		private IDocumentStoreListener[] storeListeners = new IDocumentStoreListener[0];
		private ICredentials credentials = CredentialCache.DefaultNetworkCredentials;

	    public ICredentials Credentials
	    {
	        get { return credentials; }
	        set { credentials = value; }
	    }

	    public string Identifier
		{
			get
			{
				return identifier ?? Url 
#if !CLIENT
					?? DataDirectory
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

		public string DataDirectory
		{
			get
			{
				return Configuration == null ? null : Configuration.DataDirectory;
			}
			set
			{
				if (Configuration == null)
                    Configuration = new Raven.Database.RavenConfiguration();
				Configuration.DataDirectory = value;
			}
		}
#endif
		private string connectionStringName;

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
						case "datadir":
							DataDirectory = match.Groups[2].Value.Trim();
							break;
#endif
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

		public string Url { get; set; }

		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		public void Dispose()
		{
            Stored = null;
#if !CLIENT
			if (DocumentDatabase != null)
				DocumentDatabase.Dispose();
#endif
		}

		#endregion

		public IDocumentSession OpenSession(ICredentials credentialsForSession)
		{
			if (DatabaseCommands == null)
				throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
			var session = new DocumentSession(this, storeListeners, deleteListeners, CloneDatabaseCommands());
			session.Stored += OnSessionStored;
			return session;
		}

		// Invokes the commands generator and copies over the operation headers, so that per-store headers are 
		// also available per-session.
		private IDatabaseCommands CloneDatabaseCommands()
		{
			var result = databaseCommandsGenerator();
			foreach (string header in databaseCommands.OperationsHeaders)
			{
				result.OperationsHeaders[header] = databaseCommands.OperationsHeaders[header];
			}

			return result;
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

		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			storeListeners = storeListeners.Concat(new[] {documentStoreListener}).ToArray();
			return this;
		}

		public IDocumentSession OpenSession()
		{
			if (DatabaseCommands == null)
				throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
			var session = new DocumentSession(this, storeListeners, deleteListeners, CloneDatabaseCommands());
			session.Stored += OnSessionStored;
			return session;
		}

#if !CLIENT
		public Raven.Database.DocumentDatabase DocumentDatabase { get; set; }
#endif

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

		public IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener)
		{
			deleteListeners = deleteListeners.Concat(new[] {deleteListener}).ToArray();
			return this;
		}

#if !NET_3_5

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
