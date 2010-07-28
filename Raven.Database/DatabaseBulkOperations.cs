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

		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			PerformBulkOperation(indexName, queryToDelete, allowStale, (docId, tx) =>
			{
				database.Delete(docId, null, tx);
				return new { Document = docId, Deleted = true };
			});
		}

		private void PerformBulkOperation(string index, IndexQuery indexQuery, bool allowStale, Func<string, TransactionInformation, object> batchOperation)
		{
			database.TransactionalStorage.Batch(actions =>
			{
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

				var array = new JArray();
				foreach (var documentId in queryResults)
				{
					var result = batchOperation(documentId, transactionInformation);
					array.Add(JObject.FromObject(result, new JsonSerializer { Converters = { new JsonEnumConverter() } }));
				}
			});
		}
	}
}