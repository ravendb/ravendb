using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Studio.Features.Smuggler
{
	public class SmugglerApi : SmugglerApiBase
	{
		private readonly IAsyncDatabaseCommands commands;

		private ILowLevelBulkInsertOperation bulkInsertOperation;

		public SmugglerApi(SmugglerOptions smugglerOptions, IAsyncDatabaseCommands commands)
			: base(smugglerOptions)
		{
			this.commands = commands;
		}

		public override async Task ImportData(Stream stream, SmugglerOptions options, bool importIndexes = true)
		{
			using (bulkInsertOperation = commands.GetBulkInsertOperation(new BulkInsertOptions()))
			{
				await base.ImportData(stream, options, importIndexes);
			}
		}

		protected override Task<RavenJArray> GetIndexes(int totalCount)
		{
			throw new NotImplementedException();
		}

		protected override Task<RavenJArray> GetDocuments(Guid lastEtag)
		{
			throw new NotImplementedException();
		}

		protected override Task<Guid> ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
		{
			throw new NotImplementedException();
		}

		protected override Task PutIndex(string indexName, RavenJToken index)
		{
			return commands.CreateRequest("/indexes/" + indexName, "PUT")
						   .ExecuteWriteAsync(index.Value<RavenJObject>("definition").ToString(Formatting.None));
		}

		protected override Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
		{
			return commands
				.PutAttachmentAsync(attachmentExportInfo.Key, null, attachmentExportInfo.Data, attachmentExportInfo.Metadata);
		}

		protected override Task PutDocument(RavenJObject document)
		{
			var metadata = document.Value<RavenJObject>("@metadata");
			var id = metadata.Value<string>("@id");
			document.Remove("@metadata");

			bulkInsertOperation.Write(id, metadata, document);

			return new CompletedTask();
		}

		protected override Task<DatabaseStatistics> GetStats()
		{
			return commands.GetStatisticsAsync();
		}

		protected override void ShowProgress(string format, params object[] args)
		{
			throw new NotImplementedException();
		}

		protected override void EnsureDatabaseExists()
		{
		}
	}
}
