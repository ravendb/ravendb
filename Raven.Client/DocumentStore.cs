using System;
using Raven.Database;

namespace Raven.Client
{
	public class DocumentStore : IDisposable, IDocumentStore
	{
		private readonly string localhost;
		private readonly int port;
		public IDatabaseCommands DatabaseCommands;

		public DocumentStore(string localhost, int port) : this()
		{
			this.localhost = localhost;
			this.port = port;
		}

		public DocumentStore()
		{
			Conventions = new DocumentConvention();
		}

		public string DataDirectory { get; set; }

		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		public void Dispose()
		{
            if (DatabaseCommands != null)
                DatabaseCommands.Dispose();
		}

		#endregion

		public IDocumentSession OpenSession()
		{
			return new DocumentSession(this, DatabaseCommands);
		}

        public IDocumentStore Initialise()
		{
			try
			{
				if (String.IsNullOrEmpty(localhost))
				{
					var embeddedDatabase = new DocumentDatabase(new RavenConfiguration {DataDirectory = DataDirectory});
					embeddedDatabase.SpinBackgroundWorkers();
					DatabaseCommands = new EmbededDatabaseCommands(embeddedDatabase);
				}
				else
				{
					DatabaseCommands = new ServerClient(localhost, port);
				}
				//NOTE: this should be done contitionally, index creation is expensive
				DatabaseCommands.PutIndex("getByType", "{Map: 'from entity in docs select new { entity.type };' }");
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