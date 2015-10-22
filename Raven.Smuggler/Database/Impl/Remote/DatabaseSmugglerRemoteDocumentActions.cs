using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteDocumentActions : IDatabaseSmugglerDocumentActions
	{
		private readonly BulkInsertOperation _bulkInsert;

		public DatabaseSmugglerRemoteDocumentActions(DatabaseSmugglerOptions globalOptions, DatabaseSmugglerRemoteDestinationOptions options, DocumentStore store)
		{
			_bulkInsert = store.BulkInsert(store.DefaultDatabase, new BulkInsertOptions
			{
				BatchSize = globalOptions.BatchSize,
				OverwriteExisting = true,
				Compression = options.DisableCompressionOnImport ? BulkInsertCompression.None : BulkInsertCompression.GZip,
				ChunkedBulkInsertOptions = new ChunkedBulkInsertOptions
				{
					MaxChunkVolumeInBytes = options.TotalDocumentSizeInChunkLimitInBytes,
					MaxDocumentsPerChunk = options.ChunkSize
				}
			});
		}

		public void Dispose()
		{
			_bulkInsert?.Dispose();
		}

		public Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken)
		{
			var metadata = document.Value<RavenJObject>("@metadata");
			document.Remove("@metadata");

			var id = metadata.Value<string>("@id");

			_bulkInsert.Store(document, metadata, id);
			return new CompletedTask();
		}
	}
}