// -----------------------------------------------------------------------
//  <copyright file="AdminAsyncServerClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
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

		public async Task CreateDatabaseAsync(DatabaseDocument databaseDocument)
		{
			RavenJObject doc;
			using (var req = adminRequest.CreateDatabase(databaseDocument, out doc))
			{
				await req.WriteAsync(doc.ToString(Formatting.Indented)).ConfigureAwait(false);
			}
		}

		public async Task DeleteDatabaseAsync(string databaseName, bool hardDelete = false)
		{
			using (var req = adminRequest.DeleteDatabase(databaseName, hardDelete))
			{
				await req.ExecuteRequestAsync().ConfigureAwait(false);
			}
		}

		public async Task<Operation> CompactDatabaseAsync(string databaseName)
		{
			using (var req = adminRequest.CompactDatabase(databaseName))
			{
				var json = await req.ReadResponseJsonAsync().ConfigureAwait(false);
				return new Operation((AsyncServerClient)innerAsyncServerClient.ForSystemDatabase(), json.Value<long>("OperationId"));
			}
		}

		public Task StopIndexingAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("POST", async operationMetadata =>
			{
				using (var req = adminRequest.StopIndexing(operationMetadata.Url))
				{
					await req.ExecuteRequestAsync().ConfigureAwait(false);
				}
			});
		}

		public Task StartIndexingAsync(int? maxNumberOfParallelIndexTasks = null)
		{
			return innerAsyncServerClient.ExecuteWithReplication("POST", async operationMetadata =>
			{
				using (var req = adminRequest.StartIndexing(operationMetadata.Url, maxNumberOfParallelIndexTasks))
				{
					await req.ExecuteRequestAsync().ConfigureAwait(false);
				}
			});
		}

		public Task<BuildNumber> GetBuildNumberAsync()
		{
			return innerAsyncServerClient.GetBuildNumberAsync();
		}

		public Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0)
		{
			return adminRequest.GetDatabaseNamesAsync(pageSize, start);
		}

		public async Task<AdminStatistics> GetStatisticsAsync()
		{
			using (var req = adminRequest.AdminStats())
			{
				var json = (RavenJObject)await req.ReadResponseJsonAsync().ConfigureAwait(false);
				return json.Deserialize<AdminStatistics>(innerAsyncServerClient.convention);
			}
		}

		public async Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName)
		{
			using (var request = adminRequest.StartBackup(backupLocation, databaseDocument, databaseName, incremental))
			{
				await request.WriteAsync(RavenJObject.FromObject(new DatabaseBackupRequest
				{
					BackupLocation = backupLocation,
					DatabaseDocument = databaseDocument
				})).ConfigureAwait(false);
			}
		}

		public async Task<Operation> StartRestoreAsync(DatabaseRestoreRequest restoreRequest)
		{
			using (var request = adminRequest.CreateRestoreRequest())
			{
				await request.WriteAsync(RavenJObject.FromObject(restoreRequest));

				var jsonResponse = await request.ReadResponseJsonAsync().ConfigureAwait(false);

				return new Operation((AsyncServerClient)innerAsyncServerClient.ForSystemDatabase(), jsonResponse.Value<long>("OperationId"));
			}
		}

		public Task<string> GetIndexingStatusAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("GET", async operationMetadata =>
			{
				using (var request = adminRequest.IndexingStatus(operationMetadata.Url))
				{
					var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
					return result.Value<string>("IndexingStatus");
				}
			});
		}

		public Task<RavenJObject> GetDatabaseConfigurationAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("GET", async operationMetadata =>
			{
				using (var request = adminRequest.GetDatabaseConfiguration(operationMetadata.Url))
				{
					return (RavenJObject)await request.ReadResponseJsonAsync();
				}
			});
		}

		public async Task EnsureDatabaseExistsAsync(string name, bool ignoreFailures = false)
		{
			var serverClient = (AsyncServerClient)(innerAsyncServerClient.ForSystemDatabase());

			var doc = MultiDatabase.CreateDatabaseDocument(name);

			serverClient.ForceReadFromMaster();

			var get = await serverClient.GetAsync(doc.Id).ConfigureAwait(false);
			if (get != null)
				return;

			try
			{
				await serverClient.GlobalAdmin.CreateDatabaseAsync(doc).ConfigureAwait(false);
			}
			catch (Exception)
			{
				if (ignoreFailures == false)
					throw;
			}
			await new RavenDocumentsByEntityName().ExecuteAsync(serverClient.ForDatabase(name), new DocumentConvention()).ConfigureAwait(false);
		}

	}
}
