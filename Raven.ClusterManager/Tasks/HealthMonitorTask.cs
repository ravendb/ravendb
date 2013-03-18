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
			using (var session = store.OpenSession())
			{
				var servers = session.Query<ServerRecord>()
				                     .OrderByDescending(record => record.LastOnlineTime)
									 .Take(1024)
				                     .ToList();

				foreach (var server in servers)
				{
					FetchServerDatabases(server, session);
					session.SaveChanges();
				}

				session.SaveChanges();
			}

			lastRun = DateTimeOffset.UtcNow;
		}

		public static void FetchServerDatabases(ServerRecord server, IDocumentSession session)
		{
			FetchServerDatabasesAsync(server, session).Wait();

			if (server.IsOnline && server.IsUnauthorized && server.CredentialsId == null)
			{
				var credentialses = session.Query<ServerCredentials>()
				                           .Take(16)
				                           .ToList();

				foreach (var credentials in credentialses)
				{
					server.CredentialsId = credentials.Id;
					FetchServerDatabasesAsync(server, session).Wait();
					if (server.IsUnauthorized)
					{
						server.CredentialsId = null;
					}
					if (server.IsOnline == false)
					{
						break;
					}
				}
			}

			lastRun = DateTimeOffset.UtcNow;
		}

		public static async Task FetchServerDatabasesAsync(ServerRecord server, IDocumentSession session)
		{
			var client = ServerHelpers.CreateAsyncServerClient(session, server);
			
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

		private static async Task StoreDatabaseNames(ServerRecord server, AsyncServerClient client, IDocumentSession session)
		{
			server.Databases = await client.GetDatabaseNamesAsync(1024);
			
			foreach (var databaseName in server.Databases.Concat(new[] {Constants.SystemDatabase}))
			{
				var databaseRecord = session.Load<DatabaseRecord>("databaseRecords/" + databaseName);
				if (databaseRecord == null)
				{
					databaseRecord = new DatabaseRecord {Name = databaseName, ServerId = server.Id, ServerUrl = server.Url};
					session.Store(databaseRecord);
				}
			}
		}

		private static async Task StoreActiveDatabaseNames(ServerRecord server, AsyncServerClient client, IDocumentSession session)
		{
			AdminStatistics adminStatistics = await client.Admin.GetStatisticsAsync();

			server.IsUnauthorized = false;

			server.ClusterName = adminStatistics.ClusterName;
			server.ServerName = adminStatistics.ServerName;
			server.MemoryStatistics = adminStatistics.Memory;

			foreach (var loadedDatabase in adminStatistics.LoadedDatabases)
			{
				var databaseRecord = session.Load<DatabaseRecord>("databaseRecords/" + loadedDatabase.Name);
				if (databaseRecord == null)
				{
					databaseRecord = new DatabaseRecord { Name = loadedDatabase.Name, ServerId = server.Id, ServerUrl = server.Url };
					session.Store(databaseRecord);
				}

				databaseRecord.LoadedDatabaseStatistics = loadedDatabase;
			}
			server.LoadedDatabases = adminStatistics.LoadedDatabases.Select(database => database.Name).ToArray();
		}

		private static async Task CheckReplicationStatusOfEachActiveDatabase(ServerRecord server, AsyncServerClient client, IDocumentSession session)
		{
			await HandleDatabaseInServerAsync(server, Constants.SystemDatabase.Trim('<', '>'), client, session);
			foreach (var databaseName in server.LoadedDatabases)
			{
				await HandleDatabaseInServerAsync(server, databaseName, client.ForDatabase(databaseName), session);
			}
		}

		private static async Task HandleDatabaseInServerAsync(ServerRecord server, string databaseName, IAsyncDatabaseCommands dbCmds, IDocumentSession session)
		{
			var databaseRecord = session.Load<DatabaseRecord>("databaseRecords/" + databaseName);
			if (databaseRecord == null)
				return;

			var replicationDocument = await dbCmds.GetAsync(Constants.RavenReplicationDestinations);
			if (replicationDocument == null)
				return;

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