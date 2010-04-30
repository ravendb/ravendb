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
			get { return identifier ?? Url ?? DataDirectory; }
			set { identifier = value; }
		}

		public string DataDirectory { get; set; }

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
					copy(Url ?? DataDirectory, entity);
			};
            return session;
        }

        public IDocumentStore Initialise()
		{
			try
			{
				if (String.IsNullOrEmpty(Url))
				{
					var embeddedDatabase = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDirectory});
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase);
				}
				else
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