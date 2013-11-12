﻿#if !SILVERLIGHT && !NETFX_CORE
// -----------------------------------------------------------------------
//  <copyright file="AdminDatabaseCommands.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class AdminServerClient : IAdminDatabaseCommands, IGlobalAdminDatabaseCommands
	{
		internal readonly ServerClient innerServerClient;
		private readonly AdminRequestCreator adminRequest;

		public AdminServerClient(ServerClient serverClient)
		{
			innerServerClient = serverClient;
			adminRequest =
				new AdminRequestCreator(
					(url, method) => ((ServerClient)innerServerClient.ForSystemDatabase()).CreateRequest(url, method),
					(url, method) => innerServerClient.CreateRequest(url, method),
					(currentServerUrl, requestUrl, method) => innerServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method));
		}

		public void CreateDatabase(DatabaseDocument databaseDocument)
		{
			RavenJObject doc;
			var req = adminRequest.CreateDatabase(databaseDocument, out doc);

			req.Write(doc.ToString(Formatting.Indented));
			req.ExecuteRequest();
		}

		public void DeleteDatabase(string databaseName, bool hardDelete = false)
		{
			adminRequest.DeleteDatabase(databaseName, hardDelete).ExecuteRequest();
		}

		public IDatabaseCommands Commands { get { return innerServerClient; } }

		public void CompactDatabase(string databaseName)
		{
			adminRequest.CompactDatabase(databaseName).ExecuteRequest();
		}

		public void StopIndexing()
		{
			innerServerClient.ExecuteWithReplication("POST", operationUrl => adminRequest.StopIndexing(operationUrl).ExecuteRequest());
		}

		public void StartIndexing()
		{
			innerServerClient.ExecuteWithReplication("POST", operationUrl => adminRequest.StartIndexing(operationUrl).ExecuteRequest());
		}

		public void StartBackup(string backupLocation, DatabaseDocument databaseDocument)
		{
			RavenJObject backupSettings;
			var request = adminRequest.StartBackup(backupLocation, databaseDocument, out backupSettings);

			request.Write(backupSettings.ToString(Formatting.None));
			request.ExecuteRequest();
		}

		public void StartRestore(string restoreLocation, string databaseLocation, string databaseName = null, bool defrag = false)
		{
			RavenJObject restoreSettings;
			var request = adminRequest.StartRestore(restoreLocation, databaseLocation, databaseName, defrag, out restoreSettings);

			request.Write(restoreSettings.ToString(Formatting.None));
			request.ExecuteRequest();
		}

		public string GetIndexingStatus()
		{
			return innerServerClient.ExecuteWithReplication("GET", operationUrl =>
			{
				var result = adminRequest.IndexingStatus(operationUrl).ReadResponseJson();

				return result.Value<string>("IndexingStatus");
			});
		}

		public AdminStatistics GetStatistics()
		{
			var json = (RavenJObject)adminRequest.AdminStats().ReadResponseJson();

			return json.Deserialize<AdminStatistics>(innerServerClient.convention);
		}
	}
}
#endif
