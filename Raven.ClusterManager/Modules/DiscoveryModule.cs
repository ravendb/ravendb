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
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Listeners;
using Raven.ClusterManager.Discovery;
using Raven.ClusterManager.Infrastructure;
using Raven.ClusterManager.Models;
using Nancy.ModelBinding;
using System.Linq;

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IAsyncDocumentSession session;
		private readonly static Guid SenderId = Guid.NewGuid();

		public DiscoveryModule(IAsyncDocumentSession session): base("/api/discovery")
		{
			this.session = session;

			Get["/start"] = parameters =>
			{
				var discoveryClient = new ClusterDiscoveryClient(SenderId, "http://localhost:9020/api/discovery/notify");
				Dispatcher.HandleResult(discoveryClient.PublishMyPresenceAsync());
				return "started";
			};

			Func<dynamic, Task<string>> asyncNotify = async parameters =>
			{
				var input = this.Bind<ServerRecord>("Id");

				var server = (await session.Query<ServerRecord>()
				                           .Take(1)
				                           .Where(s => s.Url == input.Url).ToListAsync()).FirstOrDefault() ?? new ServerRecord();
				this.BindTo(server, "Id");
				await session.StoreAsync(server);

				await FetchServerDatabasesAsync(server);

				return "notified";
			};

			Post["/notify"] = parameters => asyncNotify(parameters);

			/*Get["/servers"] = async parameters =>
			{
				var servers = await session.Query<ServerRecord>().ToListAsync();
				return new ClusterStatistics
				{
					Servers = servers,
				};
			};*/
		}

		private async Task FetchServerDatabasesAsync(ServerRecord server)
		{
			var documentStore = (DocumentStore)session.Advanced.DocumentStore;
			var client = new AsyncServerClient(server.Url, documentStore.Conventions, documentStore.Credentials, 
				documentStore.JsonRequestFactory, null, s => null, null, new IDocumentConflictListener[0]);
			var databaseNames = await client.GetDatabaseNamesAsync(1024);

			await HandleDatabaseInServerAsync(server, Constants.SystemDatabase, client);
			foreach (var databaseName in databaseNames)
			{
				await HandleDatabaseInServerAsync(server, databaseName, client.ForDatabase(databaseName));
			}
		}

		private async Task HandleDatabaseInServerAsync(ServerRecord server, string databaseName, IAsyncDatabaseCommands dbCmds)
		{
			var databaseRecord = new DatabaseRecord {Name = databaseName, ServerId = server.Id, ServerUrl = server.Url};
			await session.StoreAsync(databaseRecord);
			var document = await dbCmds.HeadAsync(Constants.RavenReplicationDestinations);
			server.IsOnline = true;
			server.LastOnlineTime = DateTimeOffset.Now;
			if (document != null)
			{
				databaseRecord.IsReplicationEnabled = true;
			}
		}
	}
}