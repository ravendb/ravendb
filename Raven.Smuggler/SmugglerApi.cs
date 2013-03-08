//-----------------------------------------------------------------------
// <copyright file="SmugglerApi.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Jint;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Imports;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		protected override Task<RavenJArray> GetIndexes(int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest("/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount);
			request.ExecuteRequest(reader => indexes = RavenJArray.Load(new JsonTextReader(reader)));
			return new CompletedTask<RavenJArray>(indexes);
		}

		private static string StripQuotesIfNeeded(RavenJToken value)
		{
			var str = value.ToString(Formatting.None);
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}

		public RavenConnectionStringOptions ConnectionStringOptions { get; private set; }
		private readonly HttpRavenRequestFactory httpRavenRequestFactory = new HttpRavenRequestFactory();

		private BulkInsertOperation operation;

		private DocumentStore store;
		private IAsyncDatabaseCommands Commands
		{
			get
			{
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
			using (store = CreateStore())
			{
				using (operation = store.BulkInsert(options: new BulkInsertOptions
				{
					CheckForUpdates = true
				}))
				{
					operation.Report += text => ShowProgress(text);

					await base.ImportData(options, incremental);
				}
			}
		}

		public override async Task ImportData(Stream stream, SmugglerOptions options, bool importIndexes = true)
		{
			SmugglerJintHelper.Initialize(options ?? SmugglerOptions);

			var batchSize = options != null ? options.BatchSize : SmugglerOptions.BatchSize;

			using (store = CreateStore())
			{
				using (operation = store.BulkInsert(options: new BulkInsertOptions
				{
					BatchSize = batchSize,
					CheckForUpdates = true
				}))
				{
					operation.Report += text => ShowProgress(text);

					await base.ImportData(stream, options, importIndexes);
				}
			}
		}

		public override async Task<string> ExportData(Stream stream, SmugglerOptions options, bool incremental)
		{
			using (store = CreateStore())
			{
				return await base.ExportData(stream, options, incremental);
			}
		}

		public override async Task<string> ExportData(Stream stream, SmugglerOptions options, bool incremental, bool lastEtagsFromFile)
		{
			using (store = CreateStore())
			{
				return await base.ExportData(stream, options, incremental, lastEtagsFromFile);
			}
		}

		protected override Task PutDocument(RavenJObject document)
		{
			if (document != null)
			{
				var metadata = document.Value<RavenJObject>("@metadata");
				var id = metadata.Value<string>("@id");
				document.Remove("@metadata");

				operation.Store(document, metadata, id);
			}

			return new CompletedTask();
		}

		protected HttpRavenRequest CreateRequest(string url, string method = "GET")
		{
			var builder = new StringBuilder();
			if (url.StartsWith("http", StringComparison.OrdinalIgnoreCase) == false)
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

		protected override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag)
		{
			return Commands.StreamDocsAsync(lastEtag);
		}

		protected override async Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag)
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
						lastEtag = EtagUtil.Increment(lastEtag, SmugglerOptions.BatchSize);
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

				lastEtag = Etag.Parse(attachmentInfo.Last().Value<string>("Etag"));
			}
		}

		protected override Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			if (attachmentExportInfo != null)
			{
				var request = CreateRequest("/static/" + attachmentExportInfo.Key, "PUT");
				if (attachmentExportInfo.Metadata != null)
				{
					foreach (var header in attachmentExportInfo.Metadata)
					{
						switch (header.Key)
						{
							case "Content-Type":
								request.WebRequest.ContentType = header.Value.Value<string>();
								break;
							default:
								request.WebRequest.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
								break;
						}
					}
				}

				request.Write(attachmentExportInfo.Data);
				request.ExecuteRequest();
			}

			return new CompletedTask();
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			if (index != null)
			{
				var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(index.Value<RavenJObject>("definition").ToString());

				return Commands.PutIndexAsync(indexName, indexDefinition, overwrite: true);
			}

			return FlushBatch();
		}

		protected override Task<DatabaseStatistics> GetStats()
		{
			return Commands.GetStatisticsAsync();
		}

		protected override Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
		{
			return new CompletedTask<RavenJObject>(SmugglerJintHelper.Transform(transformScript, document));
		}

		private Task FlushBatch()
		{
			return new CompletedTask();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public bool LastRequestErrored { get; set; }

		protected override Task EnsureDatabaseExists()
		{
			if (EnsuredDatabaseExists ||
				string.IsNullOrWhiteSpace(ConnectionStringOptions.DefaultDatabase))
				return new CompletedTask();

			EnsuredDatabaseExists = true;

			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(ConnectionStringOptions.Url);
			var docUrl = rootDatabaseUrl + "/docs/Raven/Databases/" + ConnectionStringOptions.DefaultDatabase;

			try
			{
				httpRavenRequestFactory.Create(docUrl, "GET", ConnectionStringOptions).ExecuteRequest();
				return new CompletedTask();
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

			return new CompletedTask();
		}
	}
}