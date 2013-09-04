//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Server.Discovery;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private static ILog logger = LogManager.GetCurrentClassLogger();
		private readonly DocumentDatabase database;
		private readonly HttpServer server;
		private ClusterDiscoveryHost discoveryHost;

		public DocumentDatabase Database
		{
			get { return database; }
		}

		public HttpServer Server
		{
			get { return server; }
		}

		public RavenDbServer(InMemoryRavenConfiguration settings)
		{
			database = new DocumentDatabase(settings);

			try
			{
				database.SpinBackgroundWorkers();
				server = new HttpServer(settings, database);
				server.StartListening();
			}
			catch (Exception)
			{
				database.Dispose();
				database = null;
				
				throw;
			}
		}

		public void Dispose()
		{
			server.Dispose();
			database.Dispose();
		}
	}
}