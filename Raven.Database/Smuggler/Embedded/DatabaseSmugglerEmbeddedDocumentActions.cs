using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
	public class DatabaseSmugglerEmbeddedDocumentActions : IDatabaseSmugglerDocumentActions
	{
		private readonly DatabaseSmugglerOptions _options;

		private readonly DocumentDatabase _database;

		private List<JsonDocument> _bulkInsertBatch = new List<JsonDocument>();

		public DatabaseSmugglerEmbeddedDocumentActions(DatabaseSmugglerOptions options, DocumentDatabase database)
		{
			_options = options;
			_database = database;
		}

		public void Dispose()
		{
			FlushBatch();
		}

		public Task WriteDocumentAsync(RavenJObject document, CancellationToken cancellationToken)
		{
			if (document != null)
			{
				var metadata = document.Value<RavenJObject>("@metadata");
				var key = metadata.Value<string>("@id");
				document.Remove("@metadata");

				_bulkInsertBatch.Add(new JsonDocument
				{
					Key = key,
					Metadata = metadata,
					DataAsJson = document,
				});

				if (_options.BatchSize > _bulkInsertBatch.Count)
					return new CompletedTask();
			}

			FlushBatch();
			return new CompletedTask();
		}

		private void FlushBatch()
		{
			if (_bulkInsertBatch == null || _bulkInsertBatch.Count <= 0)
				return;

			var batchToSave = new List<IEnumerable<JsonDocument>> { _bulkInsertBatch };
			_bulkInsertBatch = new List<JsonDocument>();
			_database.Documents.BulkInsert(new BulkInsertOptions { BatchSize = _options.BatchSize, OverwriteExisting = true }, batchToSave, Guid.NewGuid(), CancellationToken.None);
		}
	}
}