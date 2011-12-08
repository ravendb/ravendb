//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenRespondersFactory.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Web;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;

namespace Raven.Web
{
	public class ForwardToRavenRespondersFactory : IHttpHandlerFactory
	{
		internal static DocumentDatabase database;
		internal static HttpServer server;
		private static readonly object locker = new object();

		public IHttpHandler GetHandler(HttpContext context, string requestType, string url, string pathTranslated)
		{
			if (database == null)
				throw new InvalidOperationException("Database has not been initialized properly");
			return new ForwardToRavenResponders(server);
		}

		public void ReleaseHandler(IHttpHandler handler)
		{
		}

		public static void Init()
		{
			if (database != null)
				return;
			lock (locker)
			{
				if (database != null)
					return;

				var ravenConfiguration = new RavenConfiguration();
				HttpServer.RegisterHttpEndpointTarget();
				database = new DocumentDatabase(ravenConfiguration);
				database.SpinBackgroundWorkers();
				server = new HttpServer(ravenConfiguration, database);
				server.Init();
			}
		}

		public static void Shutdown()
		{
			if (database == null)
				return;
			lock (locker)
			{
				if (database == null)
					return;

				server.Dispose();
				database.Dispose();

				server = null;
				database = null;
			}
		}
	}
}