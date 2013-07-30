// -----------------------------------------------------------------------
//  <copyright file="AdminAsyncServerClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Connection.Async
{
	public class AsyncAdminServerClient : IAsyncAdminDatabaseCommands, IAsyncGlobalAdminDatabaseCommands
	{
		internal readonly AsyncServerClient innerAsyncServerClient;
		private readonly AdminRequestCreator adminRequest;

		public AsyncAdminServerClient(AsyncServerClient asyncServerClient)
		{
			innerAsyncServerClient = asyncServerClient;
			adminRequest =
				new AdminRequestCreator((url, method) => innerAsyncServerClient.ForSystemDatabase().CreateRequest(url, method),
				                        (url, method) => innerAsyncServerClient.CreateRequest(url, method),
										(currentServerUrl, requestUrl, method) => innerAsyncServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method));
		}

		public Task CreateDatabaseAsync(DatabaseDocument databaseDocument)
		{
			RavenJObject doc;
			var req = adminRequest.CreateDatabase(databaseDocument, out doc);

			return req.ExecuteWriteAsync(doc.ToString(Formatting.Indented));
		}

		public Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false)
		{
			return adminRequest.DeleteDatabase(databaseName, hardDelete).ExecuteRequestAsync();
		}

		public Task CompactDatabaseAsync(string databaseName)
		{
			return adminRequest.CompactDatabase(databaseName).ExecuteRequestAsync();
		}

		public Task StopIndexingAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("POST", operationUrl => adminRequest.StopIndexing(operationUrl).ExecuteRequestAsync());
		}

		public Task StartIndexingAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("POST", operationUrl => adminRequest.StartIndexing(operationUrl).ExecuteRequestAsync());
		}

		public async Task<AdminStatistics> GetStatisticsAsync()
		{
			var json = (RavenJObject) await adminRequest.AdminStats().ReadResponseJsonAsync();

			return json.Deserialize<AdminStatistics>(innerAsyncServerClient.convention);
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			RavenJObject backupSettings;
			var request = adminRequest.StartBackup(backupLocation, databaseDocument, out backupSettings);

			return request.ExecuteWriteAsync(backupSettings.ToString(Formatting.None));
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string databaseName = null, bool defrag = false)
		{
			RavenJObject restoreSettings;
			var request = adminRequest.StartRestore(restoreLocation, databaseLocation, databaseName, defrag, out restoreSettings);

			return request.ExecuteWriteAsync(restoreSettings.ToString(Formatting.None));
		}

		public Task<string> GetIndexingStatusAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("GET", async operationUrl =>
			{
				var result = await adminRequest.IndexingStatus(operationUrl).ReadResponseJsonAsync();

				return result.Value<string>("IndexingStatus");
			});
		}
	}
}