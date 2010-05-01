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
		public string DataDirectory { get; set; }
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
				if (String.IsNullOrEmpty(Url))
				{
					var embeddedDatabase = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDirectory});
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase);
				}
				else
#endif
				{
					DatabaseCommands = new ServerClient(Url);
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