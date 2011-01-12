//-----------------------------------------------------------------------
// <copyright file="DynamicQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
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

		public DynamicQueryRunner(DocumentDatabase database)
		{
			documentDatabase = database;
			temporaryIndexes = new ConcurrentDictionary<string, TemporaryIndexInfo>();
		}

		public QueryResult ExecuteDynamicQuery(string entityName, IndexQuery query)
		{
			// Create the map
			var map = DynamicQueryMapping.Create(documentDatabase, query, entityName);
			
			var touchTemporaryIndexResult = GetAppropriateIndexToQuery(map);

			map.IndexName = touchTemporaryIndexResult.Item1;
			// Re-write the query
			string realQuery = map.Items.Aggregate(query.Query,
												   (current, mapItem) => current.Replace(mapItem.QueryFrom, mapItem.To));

			return ExecuteActualQuery(query, map, touchTemporaryIndexResult, realQuery);
		}

		private QueryResult ExecuteActualQuery(IndexQuery query, DynamicQueryMapping map, Tuple<string, bool> touchTemporaryIndexResult, string realQuery)
		{
			// Perform the query until we have some results at least
			QueryResult result;
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
				                                });

				if (!touchTemporaryIndexResult.Item2 ||
				    !result.IsStale ||
				    result.Results.Count >= query.PageSize ||
				    sp.Elapsed.TotalSeconds > 15)
				{
					return result;
				}

				Thread.Sleep(100);
			}
		}

		private Tuple<string, bool> GetAppropriateIndexToQuery(DynamicQueryMapping map)
		{
			var appropriateIndex = new DynamicQueryOptimizer(documentDatabase).SelectAppropriateIndex(map);
			if (appropriateIndex != null)
			{
				if (appropriateIndex.StartsWith("Temp/"))// temporary index, we need to increase its usage
				{
					return  TouchTemporaryIndex(appropriateIndex, "Auto/" + appropriateIndex.Substring(5),
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
									  let timeSinceRun = DateTime.Now.Subtract(indexInfo.LastRun)
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

			if (TemporaryIndexShouldBeMadePermanent(indexInfo))
			{
				var indexDefinition = createDefinition();
				documentDatabase.DeleteIndex(temporaryIndexName);
				CreateIndex(indexDefinition, permanentIndexName);
				TemporaryIndexInfo ignored;
				temporaryIndexes.TryRemove(temporaryIndexName, out ignored);
				return Tuple.Create(permanentIndexName, false);
			}
			var temporaryIndex = documentDatabase.GetIndexDefinition(temporaryIndexName);
			if (temporaryIndex != null)
				return Tuple.Create(temporaryIndexName, false);
			CreateIndex(createDefinition(), temporaryIndexName);
			return Tuple.Create(temporaryIndexName, true);
		}

		private TemporaryIndexInfo IncrementUsageCount(string temporaryIndexName)
		{
			var indexInfo = temporaryIndexes.GetOrAdd(temporaryIndexName, s => new TemporaryIndexInfo
			{
				Created = DateTime.Now,
				RunCount = 0,
				Name = temporaryIndexName
			});
			indexInfo.LastRun = DateTime.Now;
			indexInfo.RunCount++;
			return indexInfo;
		}

		private bool TemporaryIndexShouldBeMadePermanent(TemporaryIndexInfo indexInfo)
		{
			if (indexInfo.RunCount < documentDatabase.Configuration.TempIndexPromotionMinimumQueryCount)
				return false;

			var timeSinceCreation = DateTime.Now.Subtract(indexInfo.Created);
			var score = timeSinceCreation.TotalMilliseconds / indexInfo.RunCount;

			return score < documentDatabase.Configuration.TempIndexPromotionThreshold;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		private void CreateIndex(IndexDefinition definition, string indexName)
		{
			if (documentDatabase.GetIndexDefinition(indexName) != null) // avoid race condition when creating the index
				return;

			documentDatabase.PutIndex(indexName, definition);
		}

		private class TemporaryIndexInfo
		{
			public string Name { get; set;}
			public DateTime LastRun { get; set;}
			public DateTime Created { get; set;}
			public int RunCount { get; set;}
		}
	}
}
