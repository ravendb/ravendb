//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenRespondersFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Web;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;

namespace Raven.Web
{
	public class ForwardToRavenRespondersFactory : IHttpHandlerFactory
	{
		private static readonly object locker = new object();

		static readonly RavenConfiguration ravenConfiguration;
		static readonly DocumentDatabase database;
		static readonly HttpServer server;

		static ForwardToRavenRespondersFactory()
		{
			lock (locker)
			{
				if (database != null)
					return;

				ravenConfiguration = new RavenConfiguration();
				HttpServer.RegisterHttpEndpointTarget();
				database = new DocumentDatabase(ravenConfiguration);
				database.SpinBackgroundWorkers();
				server = new HttpServer(ravenConfiguration, database);
			}
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
