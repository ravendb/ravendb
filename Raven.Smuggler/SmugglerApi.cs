//-----------------------------------------------------------------------
// <copyright file="SmugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		const int RetriesCount = 5;

		protected override Task<RavenJArray> GetIndexes(int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest("/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount);
			request.ExecuteRequest(reader => indexes = RavenJArray.Load(new JsonTextReader(reader)));
			return new CompletedTask<RavenJArray>(indexes);
		}

		public RavenConnectionStringOptions ConnectionStringOptions { get; private set; }
		private readonly HttpRavenRequestFactory httpRavenRequestFactory = new HttpRavenRequestFactory();

		private BulkInsertOperation operation;

		private DocumentStore store;
		private IAsyncDatabaseCommands Commands
		{
			get
			{
				if (store == null)
				{
					store = CreateStore();
				}

				return store.AsyncDatabaseCommands;
			}
		}

		public SmugglerApi(SmugglerOptions smugglerOptions, RavenConnectionStringOptions connectionStringOptions)
			: base(smugglerOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

		public override async Task ImportData(SmugglerOptions options, bool incremental = false)
		{
			using (var documentStore = CreateStore())
			{
				using (operation = documentStore.BulkInsert())
				{
					operation.Report += text => ShowProgress(text);

					await base.ImportData(options, incremental);
				}
			}
		}

		protected override Task PutDocument(RavenJObject document)
		{
			var metadata = document.Value<RavenJObject>("@metadata");
			var id = metadata.Value<string>("@id");
			document.Remove("@metadata");

			operation.Store(document, metadata, id);

			return new CompletedTask();
		}

		protected HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			var builder = new StringBuilder();
			if (url.StartsWith("http", StringComparison.InvariantCultureIgnoreCase) == false)
			{
				builder.Append(ConnectionStringOptions.Url);
				if (string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase) == false)
				{
					if (ConnectionStringOptions.Url.EndsWith("/") == false)
						builder.Append("/");
					builder.Append("databases/");
					builder.Append(ConnectionStringOptions.DefaultDatabase);
					builder.Append('/');
				}
			}
			builder.Append(url);
			var httpRavenRequest = httpRavenRequestFactory.Create(builder.ToString(), method, ConnectionStringOptions);
			httpRavenRequest.WebRequest.Timeout = SmugglerOptions.Timeout;
			if (LastRequestErrored)
			{
				httpRavenRequest.WebRequest.KeepAlive = false;
				httpRavenRequest.WebRequest.Timeout *= 2;
				LastRequestErrored = false;
			}
			return httpRavenRequest;
		}

		protected DocumentStore CreateStore()
		{
			var s = new DocumentStore
			{
				Url = ConnectionStringOptions.Url,
				ApiKey = ConnectionStringOptions.ApiKey,
				Credentials = ConnectionStringOptions.Credentials,
				DefaultDatabase = ConnectionStringOptions.DefaultDatabase
			};

			s.Initialize();

			return s;
		}

		protected override Task<RavenJArray> GetDocuments(Guid lastEtag)
		{
			int retries = RetriesCount;
			while (true)
			{
				try
				{
					RavenJArray documents = null;
					var request = CreateRequest("/docs?pageSize=" + SmugglerOptions.BatchSize + "&etag=" + lastEtag);
					request.ExecuteRequest(reader => documents = RavenJArray.Load(new JsonTextReader(reader)));
					return new CompletedTask<RavenJArray>(documents);
				}
				catch (Exception e)
				{
					if (retries-- == 0)
						throw;
					LastRequestErrored = true;
					ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e, RetriesCount);
				}
			}
		}

		protected override async Task<Guid> ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
		{
			int totalCount = 0;
			while (true)
			{
				RavenJArray attachmentInfo = null;
				var request = CreateRequest("/static/?pageSize=" + SmugglerOptions.BatchSize + "&etag=" + lastEtag);
				request.ExecuteRequest(reader => attachmentInfo = RavenJArray.Load(new JsonTextReader(reader)));

				if (attachmentInfo.Length == 0)
				{
					var databaseStatistics = await GetStats();
					var lastEtagComparable = new ComparableByteArray(lastEtag);
					if (lastEtagComparable.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
					{
						lastEtag = Etag.Increment(lastEtag, SmugglerOptions.BatchSize);
						ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
						continue;
					}
					ShowProgress("Done with reading attachments, total: {0}", totalCount);
					return lastEtag;
				}

				totalCount += attachmentInfo.Length;
				ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachmentInfo.Length, totalCount);
				foreach (var item in attachmentInfo)
				{
					ShowProgress("Downloading attachment: {0}", item.Value<string>("Key"));

					byte[] attachmentData = null;
					var requestData = CreateRequest("/static/" + item.Value<string>("Key"));
					requestData.ExecuteRequest(reader => attachmentData = reader.ReadData());

					new RavenJObject
						{
							{"Data", attachmentData},
							{"Metadata", item.Value<RavenJObject>("Metadata")},
							{"Key", item.Value<string>("Key")}
						}
						.WriteTo(jsonWriter);
				}

				lastEtag = new Guid(attachmentInfo.Last().Value<string>("Etag"));
			}
		}

		protected override Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			return Commands.PutAttachmentAsync(attachmentExportInfo.Key, null, attachmentExportInfo.Data, attachmentExportInfo.Metadata);
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			return Commands.CreateRequest("/indexes/" + indexName, "PUT")
						   .ExecuteWriteAsync(index.Value<RavenJObject>("definition").ToString(Formatting.None));
		}

		protected override Task<DatabaseStatistics> GetStats()
		{
			return Commands.GetStatisticsAsync();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public bool LastRequestErrored { get; set; }

		protected override void EnsureDatabaseExists()
		{
			if (EnsuredDatabaseExists ||
				string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase))
				return;

			EnsuredDatabaseExists = true;

			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(ConnectionStringOptions.Url);
			var docUrl = rootDatabaseUrl + "/docs/Raven/Databases/" + ConnectionStringOptions.DefaultDatabase;

			try
			{
				httpRavenRequestFactory.Create(docUrl, "GET", ConnectionStringOptions).ExecuteRequest();
				return;
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = CreateRequest(docUrl, "PUT");
			var document = MultiDatabase.CreateDatabaseDocument(ConnectionStringOptions.DefaultDatabase);
			request.Write(document);
			request.ExecuteRequest();
		}
	}
}