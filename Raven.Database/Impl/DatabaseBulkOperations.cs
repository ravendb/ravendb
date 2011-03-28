//-----------------------------------------------------------------------
// <copyright file="DatabaseBulkOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Http;
using Raven.Http.Json;

namespace Raven.Database.Impl
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
				FieldsToFetch = new[] { Constacts.DocumentIdFieldName },
				SortedFields = indexQuery.SortedFields
			};

			bool stale;
			var queryResults = database.QueryDocumentIds(index, bulkIndexQuery, out stale);

			if (stale && allowStale == false)
			{
				throw new InvalidOperationException(
						"Bulk operation cancelled because the index is stale and allowStale is false");
			}

			var enumerator = queryResults.GetEnumerator();
			const int batchSize = 1024;
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
