// -----------------------------------------------------------------------
//  <copyright file="ThinClient.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Tests.Migration.Utils
{
	public class ThinClient : IDisposable
	{
		private readonly string baseUrl;

		private readonly string databaseUrl;

		private readonly HttpClient httpClient;

		private readonly DocumentConvention convention;

		public ThinClient(string serverUrl, string databaseName)
		{
			baseUrl = serverUrl;
			databaseUrl = serverUrl + "/databases/" + databaseName;
			httpClient = new HttpClient();
			convention = new DocumentConvention();
		}

		public void PutDatabase(string databaseName)
		{
			var response = httpClient.PutAsync(baseUrl + "/admin/databases/" + databaseName, new JsonContent(RavenJObject.FromObject(new DatabaseDocument
																													  {
																														  Id = databaseName,
																														  Settings =
																														  {
																															  { "Raven/DataDir", "~/Databases/" + databaseName }
																														  }
																													  }))).ResultUnwrap();
			if (response.IsSuccessStatusCode == false)
				throw new InvalidOperationException(string.Format("PUT failed on '{0}'. Code: {1}.", databaseName, response.StatusCode));
		}

		public void PutIndex(AbstractIndexCreationTask index)
		{
			var response = httpClient
				.PutAsync(databaseUrl + "/indexes/" + Uri.EscapeUriString(index.IndexName), new JsonContent(RavenJObject.FromObject(index.CreateIndexDefinition())))
				.ResultUnwrap();

			if (response.IsSuccessStatusCode == false)
				throw new InvalidOperationException(string.Format("PUT failed on '{0}'. Code: {1}.", index.IndexName, response.StatusCode));
		}

		public void PutEntities(List<object> entities)
		{
			var commands = new List<PutCommandData>();
			foreach (var entity in entities)
			{
				var command = new PutCommandData();
				command.Document = RavenJObject.FromObject(entity);
				command.Key = GenerateId(entity);
				command.Metadata = CreateMetadata(entity);

				commands.Add(command);
			}

			var response = httpClient
				.PostAsync(databaseUrl + "/bulk_docs", new JsonContent(RavenJToken.FromObject(commands)))
				.ResultUnwrap();

			if (response.IsSuccessStatusCode == false)
				throw new InvalidOperationException(string.Format("BATCH failed. Code: {0}.", response.StatusCode));
		}

		public void StartBackup(string databaseName, string backupLocation, bool waitForBackupToComplete)
		{
			var response = httpClient.PostAsync(databaseUrl + "/admin/backup", new JsonContent(RavenJObject.FromObject(new DatabaseBackupRequest
																										{
																											BackupLocation = backupLocation,
																											DatabaseDocument = new DatabaseDocument
																															   {
																																   Id = databaseName
																															   }
																										}))).ResultUnwrap();

			if (response.IsSuccessStatusCode == false)
				throw new InvalidOperationException(string.Format("BACKUP failed. Code: {0}.", response.StatusCode));

			if (waitForBackupToComplete == false)
				return;

			while (true)
			{
				var json = Get(BackupStatus.RavenBackupStatusDocumentKey);
				if (json == null)
					return;

				var backupStatus = json.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
					return;

				Thread.Sleep(1000);
			}
		}

		public JsonDocument Get(string key)
		{
			var response = httpClient.GetAsync(databaseUrl + "/docs/" + Uri.EscapeUriString(key)).ResultUnwrap();

			if (response.IsSuccessStatusCode == false)
				throw new InvalidOperationException(string.Format("GET failed. Code: {0}.", response.StatusCode));

			using (var stream = response.GetResponseStreamWithHttpDecompression().ResultUnwrap())
			{
				var countingStream = new CountingStream(stream);
				var data = RavenJToken.TryLoad(countingStream);

				var docKey = Uri.UnescapeDataString(response.Headers.GetFirstValue(Constants.DocumentIdFieldName));

				response.Headers.Remove(Constants.DocumentIdFieldName);

				return new JsonDocument
				{
					DataAsJson = (RavenJObject)data,
					Metadata = response.Headers.FilterHeadersToObject(),
					Key = docKey
				};
			}
		}


		public void WaitForIndexing()
		{
			while (true)
			{
				var response = httpClient.GetAsync(databaseUrl + "/stats").ResultUnwrap();

				if (response.IsSuccessStatusCode == false)
					throw new InvalidOperationException(string.Format("STATS failed. Code: {0}.", response.StatusCode));

				using (var stream = response.GetResponseStreamWithHttpDecompression().ResultUnwrap())
				{
					var countingStream = new CountingStream(stream);
					var stats = (RavenJObject)RavenJToken.TryLoad(countingStream);

					var staleIndexes = (RavenJArray)stats["StaleIndexes"];
					if (staleIndexes.Length == 0)
						return;

					Thread.Sleep(1000);
				}
			}
		}

		public void Dispose()
		{
			if (httpClient != null)
				httpClient.Dispose();
		}

		private RavenJObject CreateMetadata(object entity)
		{
			var type = entity.GetType();
			return new RavenJObject
			       {
				       {Constants.RavenEntityName, convention.FindTypeTagName(type)},
					   {Constants.RavenClrType, convention.GetClrTypeName(type)}
			       };
		}

		private string GenerateId(object entity)
		{
			var type = entity.GetType();
			return convention.FindTypeTagName(type).ToLowerInvariant() + "/";
		}
	}
}