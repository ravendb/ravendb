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
using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
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
using Raven.Smuggler.Client;
using Raven.Smuggler.Imports;

namespace Raven.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		const int RetriesCount = 5;

		protected async override Task<RavenJArray> GetIndexes(RavenConnectionStringOptions src, int totalCount)
		{
			RavenJArray indexes = null;
			var request = CreateRequest(src, "/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount);
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


	    protected override void PurgeTombstones(ExportDataResult result)
	    {
	        throw new NotImplementedException("Purge tombstones is not supported for Command Line Smuggler");
	    }

	    protected override void ExportDeletions(JsonTextWriter jsonWriter, SmugglerOptions options, ExportDataResult result,
	                                            LastEtagsInfo maxEtagsToFetch)
	    {
	        throw new NotImplementedException("Exporting deletions is not supported for Command Line Smuggler");
	    }

	    public override LastEtagsInfo FetchCurrentMaxEtags()
	    {
	        return new LastEtagsInfo
	        {
	            LastAttachmentsDeleteEtag = null,
	            LastDocDeleteEtag = null,
	            LastAttachmentsEtag = null,
	            LastDocsEtag = null
	        };
	    }

	    protected override Task DeleteDocument(string documentId)
	    {
	        return Commands.DeleteDocumentAsync(documentId);
        }

        protected override Task DeleteAttachment(string key)
        {
            return Commands.DeleteAttachmentAsync(key, null);
	    }

	    public override async Task ImportData(SmugglerImportOptions importOptions, SmugglerOptions options, Stream stream)
		{
            SetSmugglerOptions(options);

			SmugglerJintHelper.Initialize(options);

            using (store = CreateStore(importOptions.To))
			{
				Task disposeTask;

				try
				{
					operation = new ChunkedBulkInsertOperation(store.DefaultDatabase, store, store.Listeners, new BulkInsertOptions
					{
						BatchSize = options.BatchSize,
						OverwriteExisting = true
					}, store.Changes(), options.ChunkSize);

					operation.Report += text => ShowProgress(text);

                    await base.ImportData(importOptions, options, stream);
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

		public override async Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions, SmugglerOptions options)
		{
			using (store = CreateStore(exportOptions.From))
			{
				return await base.ExportData(exportOptions, options);
			}
		}

		protected override void PutDocument(RavenJObject document, SmugglerOptions options)
		{
			if (document != null)
			{
				var metadata = document.Value<RavenJObject>("@metadata");
				var id = metadata.Value<string>("@id");
				document.Remove("@metadata");

				operation.Store(document, metadata, id);
			}
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

		protected async override Task<string> GetVersion(RavenConnectionStringOptions server)
		{
			var request = CreateRequest(server, "/build/version");
			var version = request.ExecuteRequest<RavenJObject>();

			return version["ProductVersion"].ToString();
		}

        protected HttpRavenRequest CreateRequest(RavenConnectionStringOptions connectionStringOptions, string url, string method = "GET")
		{
			var builder = new StringBuilder();
			if (url.StartsWith("http", StringComparison.OrdinalIgnoreCase) == false)
			{
				builder.Append(connectionStringOptions.Url);
				if (string.IsNullOrWhiteSpace(connectionStringOptions.DefaultDatabase) == false)
				{
					if (connectionStringOptions.Url.EndsWith("/") == false)
						builder.Append("/");
					builder.Append("databases/");
					builder.Append(connectionStringOptions.DefaultDatabase);
					builder.Append('/');
				}
			}
			builder.Append(url);
			var httpRavenRequest = httpRavenRequestFactory.Create(builder.ToString(), method, connectionStringOptions);
			httpRavenRequest.WebRequest.Timeout = (int)SmugglerOptions.Timeout.TotalMilliseconds;
			if (LastRequestErrored)
			{
				httpRavenRequest.WebRequest.KeepAlive = false;
				httpRavenRequest.WebRequest.Timeout *= 2;
				LastRequestErrored = false;
			}
			return httpRavenRequest;
		}

        protected DocumentStore CreateStore(RavenConnectionStringOptions connectionStringOptions)
        {
            var s = new DocumentStore
            {
                Url = connectionStringOptions.Url,
                ApiKey = connectionStringOptions.ApiKey,
                Credentials = connectionStringOptions.Credentials
            };

            s.Initialize();

            ValidateThatServerIsUpAndDatabaseExists(connectionStringOptions, s);

            s.DefaultDatabase = connectionStringOptions.DefaultDatabase;

            return s;
        }

		protected async override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(RavenConnectionStringOptions src, Etag lastEtag, int limit)
		{
			if (IsDocsStreamingSupported)
			{
				ShowProgress("Streaming documents from " + lastEtag);
				return await Commands.StreamDocsAsync(lastEtag, pageSize: limit);
			}

			int retries = RetriesCount;
			while (true)
			{
				try
				{
					RavenJArray documents = null;
					var url = "/docs?pageSize=" + Math.Min(SmugglerOptions.BatchSize, limit) + "&etag=" + lastEtag;
					ShowProgress("GET " + url);
					var request = CreateRequest(src, url);
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

		protected override async Task<Etag> ExportAttachments(RavenConnectionStringOptions src,JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
            if (maxEtag != null)
            {
                throw new ArgumentException("We don't support maxEtag in SmugglerApi", maxEtag);
            }

			var totalCount = 0;
			while (true)
			{
			    try
			    {
			        if (SmugglerOptions.Limit - totalCount <= 0)
			        {
			            ShowProgress("Done with reading attachments, total: {0}", totalCount);
			            return lastEtag;
			        }

			        var maxRecords = Math.Min(SmugglerOptions.Limit - totalCount, SmugglerOptions.BatchSize);
			        RavenJArray attachmentInfo = null;
			        var request = CreateRequest(src, "/static/?pageSize=" + maxRecords + "&etag=" + lastEtag);
			        request.ExecuteRequest(reader => attachmentInfo = RavenJArray.Load(new JsonTextReader(reader)));

			        if (attachmentInfo.Length == 0)
			        {
			            var databaseStatistics = await GetStats();
			            var lastEtagComparable = new ComparableByteArray(lastEtag);
			            if (lastEtagComparable.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
			            {
			                lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
			                ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
			                continue;
			            }
			            ShowProgress("Done with reading attachments, total: {0}", totalCount);
			            return lastEtag;
			        }

			        ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachmentInfo.Length,
			                     totalCount);
			        foreach (var item in attachmentInfo)
			        {
			            ShowProgress("Downloading attachment: {0}", item.Value<string>("Key"));

			            byte[] attachmentData = null;
			            var requestData = CreateRequest(src, "/static/" + item.Value<string>("Key"));
			            requestData.ExecuteRequest(reader => attachmentData = reader.ReadData());

			            new RavenJObject
			            {
			                {"Data", attachmentData},
			                {"Metadata", item.Value<RavenJObject>("Metadata")},
			                {"Key", item.Value<string>("Key")}
			            }
			                .WriteTo(jsonWriter);
			            totalCount++;
                        lastEtag = Etag.Parse(item.Value<string>("Etag"));
			        }

			    }
			    catch (Exception e)
			    {
                    ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    ShowProgress("Done with reading attachments, total: {0}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
			    }
			}
		}

		protected async override Task<RavenJArray> GetTransformers(RavenConnectionStringOptions src, int start)
		{
			if (IsTransformersSupported == false)
				return new RavenJArray();

			RavenJArray transformers = null;
			var request = CreateRequest(src, "/transformers?pageSize=" + SmugglerOptions.BatchSize + "&start=" + start);
			request.ExecuteRequest(reader => transformers = RavenJArray.Load(new JsonTextReader(reader)));
			return transformers;
		}

		protected override Task PutAttachment(RavenConnectionStringOptions dst ,AttachmentExportInfo attachmentExportInfo)
		{
			if (attachmentExportInfo != null)
			{
				var request = CreateRequest(dst, "/static/" + attachmentExportInfo.Key, "PUT");
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

        // [StringFormatMethod("format")]
		protected override void ShowProgress(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public bool LastRequestErrored { get; set; }

        protected async override Task EnsureDatabaseExists(RavenConnectionStringOptions to)
		{
			if (EnsuredDatabaseExists || string.IsNullOrWhiteSpace(to.DefaultDatabase))
				return;

			EnsuredDatabaseExists = true;

			var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(to.Url);
			var docUrl = rootDatabaseUrl + "/docs/Raven/Databases/" + to.DefaultDatabase;

			try
			{
				httpRavenRequestFactory.Create(docUrl, "GET", to).ExecuteRequest();
				return;
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null || httpWebResponse.StatusCode != HttpStatusCode.NotFound)
					throw;
			}

			var request = CreateRequest(to, docUrl, "PUT");
			var document = RavenJObject.FromObject(MultiDatabase.CreateDatabaseDocument(to.DefaultDatabase));
			document.Remove("Id");
			request.Write(document);
			request.ExecuteRequest();
		}

        private void ValidateThatServerIsUpAndDatabaseExists(RavenConnectionStringOptions server, DocumentStore s)
        {
            var shouldDispose = false;

            try
            {
                var commands = !string.IsNullOrEmpty(server.DefaultDatabase)
                                   ? s.DatabaseCommands.ForDatabase(server.DefaultDatabase)
                                   : s.DatabaseCommands;

                commands.GetStatistics(); // check if database exist
            }
            catch (Exception e)
            {
                shouldDispose = true;

                var responseException = e as ErrorResponseException;
                if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a database named"))
                    throw new SmugglerException(
                        string.Format(
                            "Smuggler does not support database creation (database '{0}' on server '{1}' must exist before running Smuggler).",
                            server.DefaultDatabase,
                            s.Url), e);


                if (e.InnerException != null)
                {
                    var webException = e.InnerException as WebException;
                    if (webException != null)
                    {
                        throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", webException.Message), webException);
                    }
                }

                throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", e.Message), e);
            }
            finally
            {
                if (shouldDispose)
                    s.Dispose();
            }
        }
	}
}