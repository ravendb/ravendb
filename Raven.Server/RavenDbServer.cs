using System;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Http;

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

			try
			{
				database.SpinBackgroundWorkers();
				server = new RavenDbHttpServer(settings, database);
				server.Start();
			}
			catch (Exception)
			{
				database.Dispose();
				database = null;
				
				throw;
			}
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
