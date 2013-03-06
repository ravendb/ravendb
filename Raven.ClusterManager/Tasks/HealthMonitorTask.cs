// -----------------------------------------------------------------------
//  <copyright file="HealthMonitorTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Listeners;
using Raven.ClusterManager.Models;

namespace Raven.ClusterManager.Tasks
{
	public class HealthMonitorTask
	{
		protected static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private readonly IDocumentStore store;
		private Timer timer;
		private static DateTimeOffset lastRun;

		static HealthMonitorTask()
		{
			MonitorInterval = TimeSpan.FromMinutes(2);
		}

		public HealthMonitorTask(IDocumentStore store)
		{
			this.store = store;
			timer = new Timer(TimerTick, null, TimeSpan.Zero, MonitorInterval);
		}

		private static TimeSpan MonitorInterval { get; set; }

		private void TimerTick(object _)
		{
			using (var session = store.OpenSession())
			{
				var servers = session.Query<ServerRecord>()
				                     .OrderByDescending(record => record.LastOnlineTime)
									 .Take(1024)
				                     .ToList();

				foreach (var server in servers)
				{
					FetchServerDatabases(server, session);
				}

				session.SaveChanges();
			}

			lastRun = DateTimeOffset.UtcNow;
		}

		public static void FetchServerDatabases(ServerRecord server, IDocumentSession session)
		{
			FetchServerDatabasesAsync(server, session)
				.Wait();

			lastRun = DateTimeOffset.UtcNow;
		}

		private static async Task FetchServerDatabasesAsync(ServerRecord server, IDocumentSession session)
		{
			var handler = new WebRequestHandler
			{
				AllowAutoRedirect = false,
			};
			var httpClient = new HttpClient(handler);
			var documentStore = (DocumentStore)session.Advanced.DocumentStore;
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.FailImmediately
			});

			var client = new AsyncServerClient(server.Url, documentStore.Conventions, documentStore.Credentials,
				documentStore.JsonRequestFactory, null, s => replicationInformer, null, new IDocumentConflictListener[0]);
			
			try
			{
				var result = await httpClient.GetAsync(server.Url + "databases");
				var resultStream = await result.Content.ReadAsStreamAsync();
				var databaseNames = resultStream.JsonDeserialization<string[]>();

				await HandleDatabaseInServerAsync(server, Constants.SystemDatabase, client, session);
				foreach (var databaseName in databaseNames)
				{
					await HandleDatabaseInServerAsync(server, databaseName, client.ForDatabase(databaseName), session);
				}
			}
			catch (HttpRequestException ex)
			{
				Log.ErrorException("Error", ex);

				var webException = ex.InnerException as WebException;
				if (webException != null)
				{
					var socketException = webException.InnerException as SocketException;
					if (socketException != null)
					{
						server.IsOnline = false;
					}
				}
			}
			catch (Exception ex)
			{
				Log.ErrorException("Error", ex);
			}
			finally
			{
				server.LastTriedToConnectAt = DateTimeOffset.UtcNow;
			}
		}

		private static async Task HandleDatabaseInServerAsync(ServerRecord server, string databaseName, IAsyncDatabaseCommands dbCmds, IDocumentSession session)
		{
			var databaseRecord = new DatabaseRecord { Name = databaseName, ServerId = server.Id, ServerUrl = server.Url };
			session.Store(databaseRecord);

			var replicationDocument = await dbCmds.GetAsync(Constants.RavenReplicationDestinations);
			server.IsOnline = true;
			server.LastOnlineTime = DateTimeOffset.UtcNow;

			if (replicationDocument != null)
			{
				databaseRecord.IsReplicationEnabled = true;
				var document = replicationDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();
				databaseRecord.ReplicationDestinations = document.Destinations;

				foreach (var replicationDestination in databaseRecord.ReplicationDestinations)
				{
					if (replicationDestination.Disabled)
						continue;

					var replicationDestinationServer = session.Load<ServerRecord>("serverRecords/" + ReplicationTask.EscapeDestinationName(replicationDestination.Url)) ?? new ServerRecord();
					if (DateTimeOffset.UtcNow - server.LastTriedToConnectAt <= MonitorInterval)
						continue;

					await FetchServerDatabasesAsync(replicationDestinationServer, session);
				}
			}
		}
	}
}