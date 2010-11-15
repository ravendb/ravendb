using System.Web;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Http;

namespace Raven.Web
{
	public class ForwardToRavenRespondersFactory : IHttpHandlerFactory
	{
		static readonly RavenConfiguration ravenConfiguration = new RavenConfiguration();
		static readonly DocumentDatabase database;
		static readonly HttpServer server;

		static ForwardToRavenRespondersFactory()
		{
			database = new DocumentDatabase(ravenConfiguration);
			database.SpinBackgroundWorkers();
			server = new RavenDbHttpServer(ravenConfiguration, database);
		}

		public IHttpHandler GetHandler(HttpContext context, string requestType, string url, string pathTranslated)
		{
			return new ForwardToRavenResponders(server);
		}

		public void ReleaseHandler(IHttpHandler handler)
		{
		}
	}
}
