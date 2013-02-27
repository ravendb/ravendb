// -----------------------------------------------------------------------
//  <copyright file="DiscoveryModule.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Nancy;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.ClusterManager.Discovery;
using Raven.ClusterManager.Models;
using Nancy.ModelBinding;
using System.Linq;

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IDocumentStore store;
		private readonly static Guid senderId = Guid.NewGuid();
		private readonly ClusterDiscoveryClient discoveryClient;

		public DiscoveryModule(IDocumentSession session, IDocumentStore store): base("/api/discovery")
		{
			this.store = store;
			discoveryClient = new ClusterDiscoveryClient(senderId, "http://localhost:9020/api/discovery/notify");

			Get["/start"] = parameters =>
			{
				discoveryClient.PublishMyPresence();
				return "started";
			};

			Post["/notify"] = parameters =>
			{
				var input = this.Bind<Server>("Id");

				var server = session.Query<Server>().FirstOrDefault(server1 => server1.Url == input.Url) ?? new Server();
				this.BindTo(server, "Id");
				session.Store(server);

				FetchServerDatabases(server);

				return "notified";
			};
		}

		private async Task FetchServerDatabases(Server server)
		{
			var databaseStore = new DocumentStore {Url = server.Url}.Initialize();
			var databaseNames = await databaseStore.AsyncDatabaseCommands.GetDatabaseNamesAsync(1024);

			var session = store.OpenSession();
			FetchDatabase(server, null, session, databaseStore);
			foreach (var databaseName in databaseNames)
			{
				FetchDatabase(server, databaseName, session, databaseStore);
			}
			session.SaveChanges();
		}

		private static void FetchDatabase(Server server, string databaseName, IDocumentSession session, IDocumentStore databaseStore)
		{
			var databaseRecord = new DatabaseRecord {Name = databaseName, ServerId = server.Id, ServerUrl = server.Url};
			session.Store(databaseRecord);

			var databaseSession = databaseStore.OpenSession(databaseRecord.Name);
			var replicationDocument = databaseSession.Load<ReplicationDocument>(Constants.RavenReplicationDestinations);
			if (replicationDocument != null)
			{
				databaseRecord.IsReplicationEnabled = true;
			}
		}
	}
}