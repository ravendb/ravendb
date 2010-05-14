using System;
using System.Net;
using Raven.Client.Client;

namespace Raven.Client.Document
{
	public class DocumentStore : IDocumentStore
	{
		public IDatabaseCommands DatabaseCommands{ get; set;}

        public event Action<string, object> Stored;

		public DocumentStore()
		{
			Conventions = new DocumentConvention();
		}

		private string identifier;
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
					var embeddedDatabase = new Raven.Database.DocumentDatabase(configuration);
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase, Conventions);
				}
				else
#endif
				{
					DatabaseCommands = new ServerClient(Url, Conventions, credentials);
				}
                if(Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
                {
                    var generator = new MultiTypeHiLoKeyGenerator(DatabaseCommands, 1024);
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
	}
}