using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		private readonly IAsyncDatabaseCommands commands;

		private readonly Action<string> output;

		private readonly IList<RavenJObject> batch;

		private static string StripQuotesIfNeeded(RavenJToken value)
		{
			var str = value.ToString(Formatting.None);
			if (str.StartsWith("\"") && str.EndsWith("\""))
				return str.Substring(1, str.Length - 2);
			return str;
		}

		public SmugglerApi(IAsyncDatabaseCommands commands, Action<string> output)
		{
			this.commands = commands;
			this.output = output;
			batch = new List<RavenJObject>();
		}

		protected override async Task<RavenJArray> GetIndexes(int totalCount)
		{
			var url = ("/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount).NoCache();
			var request = commands.CreateRequest(url, "GET");

		    return (RavenJArray)await request.ReadResponseJsonAsync();
		}

	    public override LastEtagsInfo FetchCurrentMaxEtags()
	    {
            throw new NotImplementedException("Export Deletions is not supported for command line smuggler");
	    }

	    protected override void ExportDeletions(JsonTextWriter jsonWriter, SmugglerOptions options, ExportDataResult result,
	                                            LastEtagsInfo maxEtagsToFetch)
	    {
	        throw new NotImplementedException("Export Deletions is not supported for command line smuggler");
	    }

	    protected override Task DeleteDocument(string documentId)
        {
            return commands.DeleteDocumentAsync(documentId);
        }

	    protected override void PurgeTombstones(ExportDataResult result)
	    {
	        throw new NotImplementedException();
	    }

	    protected override Task DeleteAttachment(string key)
	    {
	        return commands.DeleteAttachmentAsync(key, null);
	    }

	    protected override Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int limit)
		{
			return commands.StreamDocsAsync(lastEtag, pageSize:limit);
		}

		protected override async Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag, Etag maxEtag)
		{
            if (maxEtag != null)
            {
                throw new ArgumentException("We don't support maxEtag in SmugglerApi", "maxEtag");
            }
			int totalCount = 0;
			while (true)
			{
				RavenJArray attachmentInfo = null;

				await commands.CreateRequest("/static/?pageSize=" + SmugglerOptions.BatchSize + "&etag=" + lastEtag, "GET")
				              .ReadResponseJsonAsync()
				              .ContinueWith(task => attachmentInfo = (RavenJArray) task.Result);

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

					await commands.CreateRequest("/static/" + item.Value<string>("Key"), "GET")
					              .ReadResponseBytesAsync()
					              .ContinueWith(task => attachmentData = task.Result);

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

		protected override Task<RavenJArray> GetTransformers(int totalCount)
		{
			var url = ("/transformers?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount).NoCache();
			var request = commands.CreateRequest(url, "GET");

			return request
				.ReadResponseJsonAsync()
				.ContinueWith(task => ((RavenJArray)task.Result));
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			if (index != null)
			{
				var indexDefinition =
					JsonConvert.DeserializeObject<IndexDefinition>(index.Value<RavenJObject>("definition").ToString());

				return commands.PutIndexAsync(indexName, indexDefinition, overwrite: true);
			}

			return FlushBatch();
		}

		protected override Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			if (attachmentExportInfo != null)
			{
				var url = ("/static/" + attachmentExportInfo.Key).NoCache();
				var request = commands.CreateRequest(url, "PUT");
				if (attachmentExportInfo.Metadata != null)
				{
					foreach (var header in attachmentExportInfo.Metadata)
					{
						switch (header.Key)
						{
							case "Content-Type":
								// request.httpClient.DefaultHeaders = header.Value.Value<string>();
								break;
							default:
								// request.Headers.Add(header.Key, StripQuotesIfNeeded(header.Value));
								break;
						}
					}
				}

				return request
					.ExecuteWriteAsync(attachmentExportInfo.Data);
			}

			return FlushBatch();
		}

		protected override Task PutDocument(RavenJObject document)
		{
			if (document != null)
			{
				batch.Add(document);

				if (batch.Count >= SmugglerOptions.BatchSize)
				{
					return FlushBatch();
				}

				return new CompletedTask();
			}

			return FlushBatch();
		}

		protected override Task PutTransformer(string transformerName, RavenJToken transformer)
		{
			if (transformer != null)
			{
				var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());

				return commands.PutTransformerAsync(transformerName, transformerDefinition);
			}

			return FlushBatch();
		}

		protected override Task<string> GetVersion()
		{
			var request = commands.CreateRequest("/build/version", "GET");

			return request
				.ReadResponseJsonAsync()
				.ContinueWith(task =>
				{
					var version = (RavenJObject) task.Result;
					return version["ProductVersion"].ToString();
				});
		}

		protected override Task<DatabaseStatistics> GetStats()
		{
			return commands.GetStatisticsAsync();
		}

		protected override Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
		{
			return new CompletedTask<RavenJObject>();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			if (output != null)
				output(string.Format(format, args));
		}

		protected override Task EnsureDatabaseExists()
		{
			return new CompletedTask();
		}

		private Task FlushBatch()
		{
			if (batch.Count == 0)
				return new CompletedTask();

			var putCommands = (from doc in batch
							   let metadata = doc.Value<RavenJObject>("@metadata")
							   let removal = doc.Remove("@metadata")
							   select new PutCommandData
							   {
								   Metadata = metadata,
								   Document = doc,
								   Key = metadata.Value<string>("@id"),
							   }).ToArray();

			return commands
				.BatchAsync(putCommands)
				.ContinueOnSuccess(task => batch.Clear());
		}
	}
}
