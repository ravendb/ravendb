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
using JetBrains.Annotations;
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
		const int RetriesCount = 5;

		protected async override Task<RavenJArray> GetIndexes(int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest("/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount);
			request.ExecuteRequest(reader => indexes = RavenJArray.Load(new JsonTextReader(reader)));
			return indexes;
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

		public SmugglerApi(RavenConnectionStringOptions connectionStringOptions)
		{
			ConnectionStringOptions = connectionStringOptions;
		}

        public override async Task ImportData(SmugglerOptions options, Stream stream)
		{
            SetSmugglerOptions(options);

			SmugglerJintHelper.Initialize(options);

            using (store = CreateStore())
			{
				Task disposeTask = null;

				try
				{
					operation = store.BulkInsert(options: new BulkInsertOptions
					{
                        BatchSize = options.BatchSize,
						CheckForUpdates = true
					});

					operation.Report += text => ShowProgress(text);

                    await base.ImportData(options, stream);
				}
				finally
				{
					 disposeTask = operation.DisposeAsync();
				}

				if (disposeTask != null)
				{
					await disposeTask;
				}
			}
		}

		public override async Task<ExportDataResult> ExportData(SmugglerOptions options, PeriodicBackupStatus backupStatus = null)
		{
			using (store = CreateStore())
			{
				return await base.ExportData(options, backupStatus);
			}
		}

        public override async Task<ExportDataResult> ExportData(SmugglerOptions options, Stream stream, PeriodicBackupStatus lastEtag)
		{
			using (store = CreateStore())
			{
				return await base.ExportData(options, null, lastEtag);
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

		protected async override Task PutTransformer(string transformerName, RavenJToken transformer)
		{
			if (IsTransformersSupported == false)
				return;

			if (transformer != null)
			{
				var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());
				await Commands.PutTransformerAsync(transformerName, transformerDefinition);
			}

			await FlushBatch();
		}

		protected async override Task<string> GetVersion()
		{
			var request = CreateRequest("/build/version");
			var version = request.ExecuteRequest<RavenJObject>();

			return version["ProductVersion"].ToString();
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
			httpRavenRequest.WebRequest.Timeout = SmugglerOptions.Timeout.Milliseconds;
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

		protected async override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag)
		{
			if (IsDocsStreamingSupported)
			{
				ShowProgress("Streaming documents from " + lastEtag);
				return await Commands.StreamDocsAsync(lastEtag);
			}
			
			int retries = RetriesCount;
			while (true)
			{
				try
				{
					RavenJArray documents = null;
					var url = "/docs?pageSize=" + SmugglerOptions.BatchSize + "&etag=" + lastEtag;
					ShowProgress("GET " + url);
					var request = CreateRequest(url);
					request.ExecuteRequest(reader => documents = RavenJArray.Load(new JsonTextReader(reader)));

					return new AsyncEnumeratorBridge<RavenJObject>(documents.Values<RavenJObject>().GetEnumerator());
				}
				catch (Exception e)
				{
					if (retries-- == 0)
						throw;
					LastRequestErrored = true;
					ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e);
				}
			}
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

		protected async override Task<RavenJArray> GetTransformers(int start)
		{
			if (IsTransformersSupported == false)
				return new RavenJArray();

			RavenJArray transformers = null;
			var request = CreateRequest("/transformers?pageSize=" + SmugglerOptions.BatchSize + "&start=" + start);
			request.ExecuteRequest(reader => transformers = RavenJArray.Load(new JsonTextReader(reader)));
			return transformers;
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

		protected async override Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
		{
			return SmugglerJintHelper.Transform(transformScript, document);
		}

		private Task FlushBatch()
		{
			return new CompletedTask();
		}

        [StringFormatMethod("format")]
		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public bool LastRequestErrored { get; set; }

		protected async override Task EnsureDatabaseExists()
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
			var document = RavenJObject.FromObject(MultiDatabase.CreateDatabaseDocument(ConnectionStringOptions.DefaultDatabase));
			document.Remove("Id");
			request.Write(document);
			request.ExecuteRequest();
		}
	}
}