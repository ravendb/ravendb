using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Json;

namespace Raven.Database
{
	public class DatabaseBulkOperations
	{
		private readonly DocumentDatabase database;
		private readonly TransactionInformation transactionInformation;

		public DatabaseBulkOperations(DocumentDatabase database, TransactionInformation transactionInformation)
		{
			this.database = database;
			this.transactionInformation = transactionInformation;
		}

		public JArray DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return PerformBulkOperation(indexName, queryToDelete, allowStale, (docId, tx) =>
			{
				database.Delete(docId, null, tx);
				return new { Document = docId, Deleted = true };
			});
		}

		public JArray UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			return PerformBulkOperation(indexName, queryToUpdate, allowStale, (docId, tx) =>
			{
				var patchResult = database.ApplyPatch(docId, null, patchRequests, tx);
				return new { Document = docId, Result = patchResult };
			});
		}

		private JArray PerformBulkOperation(string index, IndexQuery indexQuery, bool allowStale, Func<string, TransactionInformation, object> batchOperation)
		{
			var array = new JArray();
			var bulkIndexQuery = new IndexQuery
			{
				Query = indexQuery.Query,
				Start = indexQuery.Start,
				Cutoff = indexQuery.Cutoff,
				PageSize = int.MaxValue,
				FieldsToFetch = new[] { "__document_id" },
				SortedFields = indexQuery.SortedFields
			};

			bool stale;
			var queryResults = database.QueryDocumentIds(index, bulkIndexQuery, out stale);

			if (stale)
			{
				if (allowStale == false)
				{
					throw new InvalidOperationException(
						"Bulk operation cancelled because the index is stale and allowStale is false");
				}
			}

			var enumerator = queryResults.GetEnumerator();
			// Todo: What's a good batch size? Make this configurable somewhere or pass it to each bulk operation?
			const int batchSize = 1280;
			while (true)
			{
				var batchCount = 0;
				database.TransactionalStorage.Batch(actions =>
				{
					while (batchCount < batchSize  && enumerator.MoveNext())
					{
						batchCount++;
						var result = batchOperation(enumerator.Current, transactionInformation);
						array.Add(JObject.FromObject(result, new JsonSerializer { Converters = { new JsonEnumConverter() } }));
					}
				});
				if (batchCount < batchSize) break;
			}
			return array;
		}

	}
}