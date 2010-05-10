using System;
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
		}

		private string identifier;
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

        public IDocumentSession OpenSession()
        {
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
					DatabaseCommands = new ServerClient(Url, Conventions);
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