//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Tenancy;
using Raven.Database.Util;
using Raven.Server.Discovery;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
		private readonly IServerThingsForTests serverThingsForTests;
		private ClusterDiscoveryHost discoveryHost;
		private readonly CompositeDisposable compositeDisposable = new CompositeDisposable();
		private readonly RavenDBOptions options;

		public RavenDbServer()
			: this(new RavenConfiguration())
		{}

		public RavenDbServer(InMemoryRavenConfiguration configuration)
		{
			var owinHttpServer = new OwinHttpServer(configuration);
			options = owinHttpServer.Options;
			compositeDisposable.Add(owinHttpServer);
			ClusterDiscovery(configuration);
			serverThingsForTests = new ServerThingsForTests(options);
		}

		//TODO DH: does this need to be exposed? Seems to be required for low level tests that the client
		//API does not expose
		public DocumentDatabase SystemDatabase
		{
			get { return options.SystemDatabase; }
		}

		//TODO DH: does this need to be exposed?
		public IServerThingsForTests Server
		{
			get { return serverThingsForTests; }
		}

		public void Dispose()
		{
			compositeDisposable.Dispose();
		}

		private void ClusterDiscovery(InMemoryRavenConfiguration configuration)
		{
			if (configuration.DisableClusterDiscovery == false)
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
							{"Url", configuration.ServerUrl},
							{"ClusterName", configuration.ClusterName},
						};
						try
						{
							HttpResponseMessage result =
								await httpClient.PostAsync(args.ClusterManagerUrl, new FormUrlEncodedContent(values));
							result.EnsureSuccessStatusCode();
						}
						catch (Exception e)
						{
							Logger.ErrorException(
								"Cannot post notification for cluster discovert to: " + configuration.ServerUrl, e);
						}
					};
					compositeDisposable.Add(discoveryHost);
				}
				catch (Exception e)
				{
					discoveryHost.Dispose();
					discoveryHost = null;

					Logger.ErrorException("Cannot setup cluster discovery", e);
				}
			}
		}

		//TODO DH: need a better name
		private class ServerThingsForTests : IServerThingsForTests
		{
			private readonly RavenDBOptions options;
			private readonly HttpServer httpServer;

			public ServerThingsForTests(RavenDBOptions options)
			{
				this.options = options;
				httpServer = httpServer;
			}

			public bool HasPendingRequests
			{
				get { return false; } //TODO DH: fix (copied from WebApiServer)
			}

			public int NumberOfRequests
			{
				get { return options.Landlord.NumberOfRequests; }
			}

			public DatabasesLandlord Landlord
			{
				get { return options.Landlord; }
			}

			public void ResetNumberOfRequests()
			{
				options.Landlord.ResetNumberOfRequests();
			}

			public Task<DocumentDatabase> GetDatabaseInternal(string databaseName)
			{
				return options.Landlord.GetDatabaseInternal(databaseName);
			}
		}
	}

	//TODO DH: need a better name
	public interface IServerThingsForTests
	{
		bool HasPendingRequests { get; }
		int NumberOfRequests { get; }
		DatabasesLandlord Landlord { get; }
		void ResetNumberOfRequests();
		Task<DocumentDatabase> GetDatabaseInternal(string databaseName);
	}
}