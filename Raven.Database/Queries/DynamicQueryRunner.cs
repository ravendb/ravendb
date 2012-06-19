//-----------------------------------------------------------------------
// <copyright file="DynamicQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using System.Diagnostics;
using System.Threading;
using Raven.Database.Indexing;

namespace Raven.Database.Queries
{
	public class DynamicQueryRunner
	{
		private readonly DocumentDatabase documentDatabase;
		private readonly ConcurrentDictionary<string, TemporaryIndexInfo> temporaryIndexes;
		private readonly object createIndexLock = new object();

		public DynamicQueryRunner(DocumentDatabase database)
		{
			documentDatabase = database;
			temporaryIndexes = new ConcurrentDictionary<string, TemporaryIndexInfo>();
		}

		public QueryResultWithIncludes ExecuteDynamicQuery(string entityName, IndexQuery query)
		{
			// Create the map
			var map = DynamicQueryMapping.Create(documentDatabase, query, entityName);

			var touchTemporaryIndexResult = GetAppropriateIndexToQuery(entityName, query, map);

			map.IndexName = touchTemporaryIndexResult.Item1;
			// Re-write the query
			string realQuery = map.Items.Aggregate(query.Query,
												   (current, mapItem) => current.Replace(mapItem.QueryFrom, mapItem.To));

			UpdateFieldNamesForSortedFields(query, map);

			// We explicitly do NOT want to update the field names of FieldsToFetch - that reads directly from the document
			//UpdateFieldsInArray(map, query.FieldsToFetch);
			
			UpdateFieldsInArray(map, query.GroupBy);

			return ExecuteActualQuery(query, map, touchTemporaryIndexResult, realQuery);
		}

		private static void UpdateFieldNamesForSortedFields(IndexQuery query, DynamicQueryMapping map)
		{
			if (query.SortedFields == null) return;
			foreach (var sortedField in query.SortedFields)
			{
				var item = map.Items.FirstOrDefault(x => x.From == sortedField.Field);
				if (item != null)
					sortedField.Field = item.To;
			}
		}

		private static void UpdateFieldsInArray(DynamicQueryMapping map, string[] fields)
		{
			if (fields == null)
				return;
			for (var i = 0; i < fields.Length; i++)
			{
				var item = map.Items.FirstOrDefault(x => x.From == fields[i]);
				if (item != null)
					fields[i] = item.To;
			}
		}

		private QueryResultWithIncludes ExecuteActualQuery(IndexQuery query, DynamicQueryMapping map, Tuple<string, bool> touchTemporaryIndexResult, string realQuery)
		{
			// Perform the query until we have some results at least
			QueryResultWithIncludes result;
			var sp = Stopwatch.StartNew();
			while (true)
			{
				result = documentDatabase.Query(map.IndexName,
												new IndexQuery
												{
													Cutoff = query.Cutoff,
													PageSize = query.PageSize,
													Query = realQuery,
													Start = query.Start,
													FieldsToFetch = query.FieldsToFetch,
													GroupBy = query.GroupBy,
													AggregationOperation = query.AggregationOperation,
													SortedFields = query.SortedFields,
													DefaultField = query.DefaultField
												});

				if (!touchTemporaryIndexResult.Item2 ||
					!result.IsStale ||
					(result.Results.Count >= query.PageSize && query.PageSize > 0) ||
					sp.Elapsed.TotalSeconds > 15)
				{
					return result;
				}

				Thread.Sleep(100);
			}
		}

		private Tuple<string, bool> GetAppropriateIndexToQuery(string entityName, IndexQuery query, DynamicQueryMapping map)
		{
			var appropriateIndex = new DynamicQueryOptimizer(documentDatabase).SelectAppropriateIndex(entityName, query);
			if (appropriateIndex != null)
			{
				if (appropriateIndex.StartsWith("Temp/"))// temporary index, we need to increase its usage
				{
					return TouchTemporaryIndex(appropriateIndex, "Auto/" + appropriateIndex.Substring(5),
																	() => documentDatabase.IndexDefinitionStorage.GetIndexDefinition(appropriateIndex));
				}
				return Tuple.Create(appropriateIndex, false);
			}
			return TouchTemporaryIndex(map.TemporaryIndexName, map.PermanentIndexName,
															map.CreateIndexDefinition);
		}

		public void CleanupCache()
		{
			foreach (var indexInfo in from index in temporaryIndexes
									  let indexInfo = index.Value
									  let timeSinceRun = SystemTime.Now.Subtract(indexInfo.LastRun)
									  where timeSinceRun > documentDatabase.Configuration.TempIndexCleanupThreshold
									  select indexInfo)
			{
				documentDatabase.DeleteIndex(indexInfo.Name);
				TemporaryIndexInfo ignored;
				temporaryIndexes.TryRemove(indexInfo.Name, out ignored);
			}
		}

		private Tuple<string, bool> TouchTemporaryIndex(string temporaryIndexName, string permanentIndexName, Func<IndexDefinition> createDefinition)
		{
			var indexInfo = IncrementUsageCount(temporaryIndexName);

			if (documentDatabase.GetIndexDefinition(permanentIndexName) != null)
				return Tuple.Create(permanentIndexName, false);

			if (TemporaryIndexShouldBeMadePermanent(indexInfo))
			{
				TempIndexToPermanentIndex(temporaryIndexName, permanentIndexName, createDefinition);
				return Tuple.Create(permanentIndexName, false);
			}

			// we make the check here to avoid locking if the index already exists
			var temporaryIndex = documentDatabase.GetIndexDefinition(temporaryIndexName);
			if (temporaryIndex != null)
				return Tuple.Create(temporaryIndexName, false);

			lock (createIndexLock)
			{
				// double checked locking, to ensure that we only create the index once
				temporaryIndex = documentDatabase.GetIndexDefinition(temporaryIndexName);
				if (temporaryIndex != null)
					return Tuple.Create(temporaryIndexName, false);

				documentDatabase.PutIndex(temporaryIndexName, createDefinition());

				return Tuple.Create(temporaryIndexName, true);
			}

		}

		private void TempIndexToPermanentIndex(string temporaryIndexName, string permanentIndexName, Func<IndexDefinition> createDefinition)
		{
			if (documentDatabase.GetIndexDefinition(permanentIndexName) != null)
				return;

			lock (createIndexLock)
			{
				if (documentDatabase.GetIndexDefinition(permanentIndexName) != null)
					return;

				var indexDefinition = createDefinition();
				documentDatabase.DeleteIndex(temporaryIndexName);
				documentDatabase.PutIndex(permanentIndexName, indexDefinition);
				TemporaryIndexInfo ignored;
				temporaryIndexes.TryRemove(temporaryIndexName, out ignored);
			}
		}

		private TemporaryIndexInfo IncrementUsageCount(string temporaryIndexName)
		{
			var indexInfo = temporaryIndexes.GetOrAdd(temporaryIndexName, s => new TemporaryIndexInfo
			{
				Created = SystemTime.Now,
				RunCount = 0,
				Name = temporaryIndexName
			});
			indexInfo.LastRun = SystemTime.Now;
			Interlocked.Increment(ref indexInfo.RunCount);
			return indexInfo;
		}

		private bool TemporaryIndexShouldBeMadePermanent(TemporaryIndexInfo indexInfo)
		{
			if (indexInfo.RunCount < documentDatabase.Configuration.TempIndexPromotionMinimumQueryCount)
				return false;

			var timeSinceCreation = SystemTime.Now.Subtract(indexInfo.Created);
			var score = timeSinceCreation.TotalMilliseconds / indexInfo.RunCount;

			return score < documentDatabase.Configuration.TempIndexPromotionThreshold;
		}

		private class TemporaryIndexInfo
		{
			public string Name;
			public DateTime LastRun;
			public DateTime Created;
			public int RunCount;
		}
	}
}
