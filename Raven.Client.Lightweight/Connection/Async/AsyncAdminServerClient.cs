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

		public Task CreateDatabaseAsync(DatabaseDocument databaseDocument)
		{
			RavenJObject doc;
			var req = adminRequest.CreateDatabase(databaseDocument, out doc);

			return req.WriteAsync(doc.ToString(Formatting.Indented));
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
			return innerAsyncServerClient.ExecuteWithReplication("POST", operationMetadata => adminRequest.StopIndexing(operationMetadata.Url).ExecuteRequestAsync());
		}

		public Task StartIndexingAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("POST", operationMetadata => adminRequest.StartIndexing(operationMetadata.Url).ExecuteRequestAsync());
		}

		public async Task<AdminStatistics> GetStatisticsAsync()
		{
			var json = (RavenJObject) await adminRequest.AdminStats().ReadResponseJsonAsync();

			return json.Deserialize<AdminStatistics>(innerAsyncServerClient.convention);
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument, bool incremental, string databaseName)
		{
		    var request = adminRequest.StartBackup(backupLocation, databaseDocument, databaseName, incremental);

            return request.WriteAsync(RavenJObject.FromObject(new BackupRequest
            {
                BackupLocation = backupLocation,
                DatabaseDocument = databaseDocument
            }));
		}

		public Task StartRestoreAsync(RestoreRequest restoreRequest)
		{
		    var request = adminRequest.CreateRestoreRequest();

			return request.WriteAsync(RavenJObject.FromObject(restoreRequest));
		}

		public Task<string> GetIndexingStatusAsync()
		{
			return innerAsyncServerClient.ExecuteWithReplication("GET", async operationMetadata =>
			{
				var result = await adminRequest.IndexingStatus(operationMetadata.Url).ReadResponseJsonAsync();

				return result.Value<string>("IndexingStatus");
			});
		}

		
		public async Task EnsureDatabaseExistsAsync(string name, bool ignoreFailures = false)
		{
			var serverClient = (AsyncServerClient) (innerAsyncServerClient.ForSystemDatabase());

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
