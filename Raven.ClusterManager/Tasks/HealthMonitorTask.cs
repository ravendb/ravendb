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
using Raven.Client.Connection.Async;
using Raven.ClusterManager.Models;

namespace Raven.ClusterManager.Tasks
{
	public class HealthMonitorTask
	{
		protected static readonly ILog Log = LogManager.GetCurrentClassLogger();

		private readonly IDocumentStore store;
		private Timer timer;
		private volatile bool running;

		static HealthMonitorTask()
		{
			MonitorInterval = TimeSpan.FromMinutes(1);
		}

		public HealthMonitorTask(IDocumentStore store)
		{
			this.store = store;
			timer = new Timer(TimerTick, null, TimeSpan.Zero, MonitorInterval);
		}

		private static TimeSpan MonitorInterval { get; set; }

		private void TimerTick(object _)
		{
			if (running)
				return;
			running = true;
			CheckHealthAsync()
				.ContinueWith(task =>
				{
					running = false;
				});
		}

		private async Task CheckHealthAsync()
		{
			using (var session = store.OpenAsyncSession())
			{
				var servers = await session.Query<ServerRecord>()
				                           .OrderByDescending(record => record.LastOnlineTime)
				                           .Take(1024)
				                           .ToListAsync();

				var tasks = servers.Select(server => FetchServerDatabases(server, store)).ToArray();
				Task.WaitAll(tasks);
			}
		}

		public static async Task FetchServerDatabases(ServerRecord server, IDocumentStore documentStore)
		{
			using (var session = documentStore.OpenAsyncSession())
			{
				await session.StoreAsync(server);
				await FetchServerDatabasesAsync(server, session);
				await session.SaveChangesAsync();
			}

			if (server.IsOnline && server.IsUnauthorized && server.CredentialsId == null)
			{
				using (var session = documentStore.OpenAsyncSession())
				{
					await session.StoreAsync(server);

					var credentialses = await session.Query<ServerCredentials>()
					                                 .Take(16)
					                                 .ToListAsync();

					foreach (var credentials in credentialses)
					{
						server.CredentialsId = credentials.Id;
						await FetchServerDatabasesAsync(server, session);
						if (server.IsUnauthorized)
						{
							server.CredentialsId = null;
						}
						if (server.IsOnline == false)
						{
							break;
						}
					}

					await session.SaveChangesAsync();
				}
			}
		}

		public static async Task FetchServerDatabasesAsync(ServerRecord server, IAsyncDocumentSession session)
		{
			var client = await ServerHelpers.CreateAsyncServerClient(session, server);
			
			try
			{
				await StoreDatabaseNames(server, client, session);
				// Mark server as online now, so if one of the later steps throw we'll have this value.
				server.NotifyServerIsOnline();

				await StoreActiveDatabaseNames(server, client, session);
				await CheckReplicationStatusOfEachActiveDatabase(server, client, session);

				// Mark server as online at the LastOnlineTime.
				server.NotifyServerIsOnline();
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
			catch (AggregateException ex)
			{
				Log.ErrorException("Error", ex);

				var exception = ex.ExtractSingleInnerException();

				var webException = exception as WebException;
				if (webException != null)
				{
					var response = webException.Response as HttpWebResponse;
					if (response != null && response.StatusCode == HttpStatusCode.Unauthorized)
					{
						server.IsUnauthorized = true;
					}
					else
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

		private static async Task StoreDatabaseNames(ServerRecord server, AsyncServerClient client, IAsyncDocumentSession session)
		{
			server.Databases = await client.GetDatabaseNamesAsync(1024);
			
			foreach (var databaseName in server.Databases.Concat(new[] {Constants.SystemDatabase}))
			{
				var databaseRecord = await session.LoadAsync<DatabaseRecord>(server.Id + "/" + databaseName);
				if (databaseRecord == null)
				{
					databaseRecord = new DatabaseRecord {Name = databaseName, ServerId = server.Id, ServerUrl = server.Url};
					await session.StoreAsync(databaseRecord);
				}
			}
		}

		private static async Task StoreActiveDatabaseNames(ServerRecord server, AsyncServerClient client, IAsyncDocumentSession session)
		{
			AdminStatistics adminStatistics = await client.GlobalAdmin.GetStatisticsAsync();

			server.IsUnauthorized = false;

			server.ClusterName = adminStatistics.ClusterName;
			server.ServerName = adminStatistics.ServerName;
			server.MemoryStatistics = adminStatistics.Memory;

			foreach (var loadedDatabase in adminStatistics.LoadedDatabases)
			{
				var databaseRecord = await session.LoadAsync<DatabaseRecord>(server.Id + "/" + loadedDatabase.Name);
				if (databaseRecord == null)
				{
					databaseRecord = new DatabaseRecord { Name = loadedDatabase.Name, ServerId = server.Id, ServerUrl = server.Url };
					await session.StoreAsync(databaseRecord);
				}

				databaseRecord.LoadedDatabaseStatistics = loadedDatabase;
			}
			server.LoadedDatabases = adminStatistics.LoadedDatabases.Select(database => database.Name).ToArray();
		}

		private static async Task CheckReplicationStatusOfEachActiveDatabase(ServerRecord server, AsyncServerClient client, IAsyncDocumentSession session)
		{
			await HandleDatabaseInServerAsync(server, Constants.SystemDatabase, client, session);
			foreach (var databaseName in server.LoadedDatabases)
			{
				await HandleDatabaseInServerAsync(server, databaseName, client.ForDatabase(databaseName), session);
			}
		}

		private static async Task HandleDatabaseInServerAsync(ServerRecord server, string databaseName, IAsyncDatabaseCommands dbCmds, IAsyncDocumentSession session)
		{
			var databaseRecord = await session.LoadAsync<DatabaseRecord>(server.Id + "/" + databaseName);
			if (databaseRecord == null)
				return;

			var replicationDocument = await dbCmds.GetAsync(Constants.RavenReplicationDestinations);
			if (replicationDocument == null)
				return;

			databaseRecord.IsReplicationEnabled = true;
			var document = replicationDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();
			databaseRecord.ReplicationDestinations = document.Destinations;

			var replicationStatistics = await dbCmds.Info.GetReplicationInfoAsync();
			if (replicationStatistics != null)
			{
				databaseRecord.ReplicationStatistics = replicationStatistics;
			}

			// Monitor the replicated destinations
			foreach (var replicationDestination in databaseRecord.ReplicationDestinations)
			{
				if (replicationDestination.Disabled)
					continue;

				var url = replicationDestination.Url;
				var databasesIndex = url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
				if (databasesIndex > 0)
				{
					url = url.Substring(0, databasesIndex);
				}
				var replicationDestinationServer = await session.LoadAsync<ServerRecord>("serverRecords/" + ReplicationTask.EscapeDestinationName(url));
				if (replicationDestinationServer == null)
				{
					replicationDestinationServer = new ServerRecord
					{
						Url = url,
					};
					await session.StoreAsync(replicationDestinationServer);
				}
				else
				{
					if (DateTimeOffset.UtcNow - server.LastTriedToConnectAt <= TimeSpan.FromHours(1))
						continue;
				}
				
				await FetchServerDatabasesAsync(replicationDestinationServer, session);
			}
		}
	}
}