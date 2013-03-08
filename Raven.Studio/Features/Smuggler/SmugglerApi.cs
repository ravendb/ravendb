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

		public SmugglerApi(SmugglerOptions smugglerOptions, IAsyncDatabaseCommands commands, Action<string> output)
			: base(smugglerOptions)
		{
			this.commands = commands;
			this.output = output;
			batch = new List<RavenJObject>();
		}

		protected override Task<RavenJArray> GetIndexes(int totalCount)
		{
			var url = ("/indexes?pageSize=" + SmugglerOptions.BatchSize + "&start=" + totalCount).NoCache();
			var request = commands.CreateRequest(url, "GET");

			return request
				.ReadResponseJsonAsync()
				.ContinueWith(task => ((RavenJArray) task.Result));
		}

		protected override Task<RavenJArray> GetDocuments(Etag lastEtag)
		{
			throw new NotImplementedException();
		}

		protected override Task<Etag> ExportAttachments(JsonTextWriter jsonWriter, Etag lastEtag)
		{
			throw new NotImplementedException();
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
								request.ContentType = header.Value.Value<string>();
								break;
							default:
								request.Headers[header.Key] = StripQuotesIfNeeded(header.Value);
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

		protected override Task<DatabaseStatistics> GetStats()
		{
			return commands.GetStatisticsAsync();
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
