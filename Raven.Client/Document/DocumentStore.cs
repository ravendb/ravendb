using System;
using System.Net;
using Raven.Client.Client;
using Raven.Database;

namespace Raven.Client.Document
{
	public class DocumentStore : IDocumentStore
	{
		public IDatabaseCommands DatabaseCommands{ get; set;}

        public event Action<string, object> Stored;

		public DocumentStore()
		{
			Conventions = new DocumentConvention();

            // Create a hi lo key generator and set that up as the default key generator
            // Really not sure where to put this, as there is no precedent to follow
            keyGenerator = new HiLoKeyGenerator(10, this);
            keyGenerator.SetupConventions(Conventions);
		}

		private string identifier;
        private ICredentials credentials = CredentialCache.DefaultNetworkCredentials;
        private HiLoKeyGenerator keyGenerator;

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
		private RavenConfiguration configuration;
		public RavenConfiguration Configuration
		{
			get
			{
				if(configuration == null)
					configuration = new RavenConfiguration();
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
					Configuration = new RavenConfiguration();
				Configuration.DataDirectory = value;
			}
		}
#endif
		public string Url { get; set; }

		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		public void Dispose()
		{
            Stored = null;

            if (DatabaseCommands != null)
                DatabaseCommands.Dispose();
		}

		#endregion

        public IDocumentSession OpenSession(ICredentials credentialsForSession)
        {

            if (DatabaseCommands == null)
                throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
            var session = new DocumentSession(this, DatabaseCommands.With(credentialsForSession));
            session.Stored += entity =>
            {
                var copy = Stored;
                if (copy != null)
                    copy(Identifier, entity);
            };
            return session;
        }

        public IDocumentSession OpenSession()
        {
            if(DatabaseCommands == null)
                throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
            var session = new DocumentSession(this, DatabaseCommands);
			session.Stored += entity =>
			{
				var copy = Stored;
				if (copy != null) 
					copy(Identifier, entity);
			};
            return session;
        }

        public IDocumentStore Initialise()
		{
			try
			{
#if !CLIENT
				if (configuration != null)
				{
					var embeddedDatabase = new DocumentDatabase(configuration);
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase, Conventions);
				}
				else
#endif
				{
					DatabaseCommands = new ServerClient(Url, Conventions, credentials);
				}
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
		}
	}
}