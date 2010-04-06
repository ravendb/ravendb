using System;
using Raven.Database;

namespace Raven.Client
{
	public class DocumentStore : IDisposable
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
			var disposable = DatabaseCommands as IDisposable;
			if (disposable != null)
				disposable.Dispose();
		}

		#endregion

		public DocumentSession OpenSession()
		{
			return new DocumentSession(this, DatabaseCommands);
		}

		public void Initialise()
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
			catch (Exception e)
			{
				Dispose();
				throw;
			}
		}

		public void Delete(Guid id)
		{
			DatabaseCommands.Delete(id.ToString(), null);
		}
	}
}