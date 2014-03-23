//-----------------------------------------------------------------------
// <copyright file="DatabaseBulkOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Database.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public class DatabaseBulkOperations
	{
		private readonly DocumentDatabase database;
		private readonly TransactionInformation transactionInformation;
		private readonly CancellationToken token;
		private readonly CancellationTokenSourceExtensions.CancellationTimeout timeout;

		public DatabaseBulkOperations(DocumentDatabase database, TransactionInformation transactionInformation, CancellationToken token, CancellationTokenSourceExtensions.CancellationTimeout timeout)
		{
			this.database = database;
			this.transactionInformation = transactionInformation;
			this.token = token;
			this.timeout = timeout;
		}

		public RavenJArray DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			return PerformBulkOperation(indexName, queryToDelete, allowStale, (docId, tx) =>
			{
				database.Documents.Delete(docId, null, tx);
				return new { Document = docId, Deleted = true };
			});
		}

		public RavenJArray UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
		{
			return PerformBulkOperation(indexName, queryToUpdate, allowStale, (docId, tx) =>
			{
				var patchResult = database.Patches.ApplyPatch(docId, null, patchRequests, tx);
				return new { Document = docId, Result = patchResult };
			});
		}

		public RavenJArray UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			return PerformBulkOperation(indexName, queryToUpdate, allowStale, (docId, tx) =>
			{
				var patchResult = database.Patches.ApplyPatch(docId, null, patch, tx);
				return new { Document = docId, Result = patchResult.Item1, Debug = patchResult.Item2 };
			});
		}

		private RavenJArray PerformBulkOperation(string index, IndexQuery indexQuery, bool allowStale, Func<string, TransactionInformation, object> batchOperation)
		{
			var array = new RavenJArray();
			var bulkIndexQuery = new IndexQuery
			{
				Query = indexQuery.Query,
				Start = indexQuery.Start,
				Cutoff = indexQuery.Cutoff,
                WaitForNonStaleResultsAsOfNow = indexQuery.WaitForNonStaleResultsAsOfNow,
				PageSize = int.MaxValue,
				FieldsToFetch = new[] { Constants.DocumentIdFieldName },
				SortedFields = indexQuery.SortedFields,
				HighlighterPreTags = indexQuery.HighlighterPreTags,
				HighlighterPostTags = indexQuery.HighlighterPostTags,
				HighlightedFields = indexQuery.HighlightedFields,
				SortHints = indexQuery.SortHints
			};

			bool stale;
			var queryResults = database.Queries.QueryDocumentIds(index, bulkIndexQuery, token, out stale);

			if (stale && allowStale == false)
			{
				throw new InvalidOperationException(
						"Bulk operation cancelled because the index is stale and allowStale is false");
			}

			const int batchSize = 1024;
			using (var enumerator = queryResults.GetEnumerator())
			{
				while (true)
				{
					if (timeout != null)
						timeout.Delay();
					var batchCount = 0;
					database.TransactionalStorage.Batch(actions =>
					{
						while (batchCount < batchSize && enumerator.MoveNext())
						{
							batchCount++;
							var result = batchOperation(enumerator.Current, transactionInformation);
							array.Add(RavenJObject.FromObject(result));
						}
					});
					if (batchCount < batchSize) break;
				}
			}
			return array;
		}
	}
}
