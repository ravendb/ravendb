using System;
using Raven.Client.Client;
using Raven.Database;

namespace Raven.Client.Document
{
	public class DocumentStore : IDocumentStore
	{
		private readonly string server;
		private readonly int port;
		public IDatabaseCommands DatabaseCommands;

        public event Action<string, int, object> Stored;

		public DocumentStore(string server, int port) : this()
		{
			this.server = server;
			this.port = port;
		}

		public DocumentStore()
		{
			Conventions = new DocumentConvention();
		}

        public string Identifier { get; set; }

		public string DataDirectory { get; set; }

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
            session.Stored += entity => { if (Stored != null) Stored(server, port, entity); };
            return session;
        }

        public IDocumentStore Initialise()
		{
			try
			{
				if (String.IsNullOrEmpty(server))
				{
					var embeddedDatabase = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDirectory});
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase);
				}
				else
				{
					DatabaseCommands = new ServerClient(server, port);
				}
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
		}

		public void Delete(Guid id)
		{
			DatabaseCommands.Delete(id.ToString(), null);
		}
	}
}