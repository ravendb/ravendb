// -----------------------------------------------------------------------
//  <copyright file="DiscoveryModule.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Nancy;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Listeners;
using Raven.ClusterManager.Discovery;
using Raven.ClusterManager.Models;
using Nancy.ModelBinding;
using System.Linq;

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IDocumentSession session;
		private readonly static Guid SenderId = Guid.NewGuid();

		public DiscoveryModule(IDocumentSession session)
			: base("/api/discovery")
		{
			this.session = session;

			Get["/start"] = parameters =>
			{
				var discoveryClient = new ClusterDiscoveryClient(SenderId, "http://localhost:9020/api/discovery/notify");
				discoveryClient.PublishMyPresenceAsync();
				return "started";
			};

			Post["/notify"] = parameters =>
			{
				var input = this.Bind<ServerRecord>("Id");

				var server = session.Query<ServerRecord>().FirstOrDefault(s => s.Url == input.Url) ?? new ServerRecord();
				this.BindTo(server, "Id");
				session.Store(server);

				try
				{
					FetchServerDatabasesAsync(server)
						.Wait();
				}
				catch (SocketException)
				{
					server.IsOnline = false;
				}

				return "notified";
			};

			Get["/servers"] = parameters =>
			{
				var servers = this.session.Query<ServerRecord>().ToList();
				return new ClusterStatistics
				{
					Servers = servers,
				};
			};
		}

		private async Task FetchServerDatabasesAsync(ServerRecord server)
		{
			var documentStore = (DocumentStore)session.Advanced.DocumentStore;
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.FailImmediately
			});

			var client = new AsyncServerClient(server.Url, documentStore.Conventions, documentStore.Credentials,
				documentStore.JsonRequestFactory, null, s => replicationInformer, null, new IDocumentConflictListener[0]);
			var databaseNames = await client.GetDatabaseNamesAsync(1024);

			await HandleDatabaseInServerAsync(server, Constants.SystemDatabase, client);
			foreach (var databaseName in databaseNames)
			{
				await HandleDatabaseInServerAsync(server, databaseName, client.ForDatabase(databaseName));
			}
		}

		private async Task HandleDatabaseInServerAsync(ServerRecord server, string databaseName, IAsyncDatabaseCommands dbCmds)
		{
			var databaseRecord = new DatabaseRecord { Name = databaseName, ServerId = server.Id, ServerUrl = server.Url };
			session.Store(databaseRecord);
			var replicationDocument = await dbCmds.GetAsync(Constants.RavenReplicationDestinations);
			server.IsOnline = true;
			server.LastOnlineTime = DateTimeOffset.Now;
			if (replicationDocument != null)
			{
				databaseRecord.IsReplicationEnabled = true;
			}
		}
	}
}