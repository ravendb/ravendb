using System;
using Raven.Database;
using Raven.Database.Server;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private readonly DocumentDatabase database;
		private readonly HttpServer server;

		public DocumentDatabase Database
		{
			get { return database; }
		}

		public HttpServer Server
		{
			get { return server; }
		}

		public RavenDbServer(RavenConfiguration settings)
		{
			settings.LoadLoggingSettings();
			database = new DocumentDatabase(settings);
			database.SpinBackgroundWorkers();
			server = new HttpServer(settings, database);
			server.Start();
		}

		#region IDisposable Members

		public void Dispose()
		{
			server.Dispose();
			database.Dispose();
		}

		#endregion

	}
}