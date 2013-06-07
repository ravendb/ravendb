//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using System.Windows.Forms;
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

			ClusterDiscovery(settings);
		}

		private void ClusterDiscovery(InMemoryRavenConfiguration settings)
		{
			if (settings.DisableClusterDiscovery == false)
			{
				discoveryHost = new ClusterDiscoveryHost();
				try
				{
					discoveryHost.Start();
					discoveryHost.ClientDiscovered += async (sender, args) =>
					{
						var httpClient = new HttpClient(new HttpClientHandler());
						var values = new Dictionary<string, string>
						{
							{"Url", settings.ServerUrl},
							{"ClusterName", settings.ClusterName},
						};
						try
						{
							var result = await httpClient.PostAsync(args.ClusterManagerUrl, new FormUrlEncodedContent(values));
							result.EnsureSuccessStatusCode();
						}
						catch (Exception e)
						{
							logger.ErrorException("Cannot post notification for cluster discovert to: " + settings.ServerUrl, e);
						}
					};
				}
				catch (Exception e)
				{
					discoveryHost.Dispose();
					discoveryHost = null;

					logger.ErrorException("Cannot setup cluster discovery" , e);
				}
			}
		}

		public void Dispose()
		{
			server.Dispose();
			database.Dispose();
		}
	}
}
