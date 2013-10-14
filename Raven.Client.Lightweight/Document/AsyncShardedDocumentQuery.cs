//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;
using Raven.Client.Shard;
using Raven.Client.Extensions;
using Raven.Client.WinRT.MissingFromWinRT;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	public class AsyncShardedDocumentQuery<T> : AsyncDocumentQuery<T>
	{
		private readonly Func<ShardRequestData, IList<Tuple<string, IAsyncDatabaseCommands>>> getShardsToOperateOn;
		private readonly ShardStrategy shardStrategy;

		private List<QueryOperation> shardQueryOperations;

		private IList<IAsyncDatabaseCommands> databaseCommands;
		private IList<IAsyncDatabaseCommands> ShardDatabaseCommands
		{
			get
			{
				if (databaseCommands == null)
				{
					var shardsToOperateOn = getShardsToOperateOn(new ShardRequestData { EntityType = typeof(T), Query = IndexQuery , IndexName = indexName});
					databaseCommands = shardsToOperateOn.Select(x => x.Item2).ToList();
				}
				return databaseCommands;
			}
		}

		private IndexQuery indexQuery;
		private IndexQuery IndexQuery
		{
			get { return indexQuery ?? (indexQuery = GenerateIndexQuery(queryText.ToString())); }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery{T}"/> class.
		/// </summary>
		public AsyncShardedDocumentQuery(InMemoryDocumentSessionOperations session, Func<ShardRequestData, IList<Tuple<string, IAsyncDatabaseCommands>>> getShardsToOperateOn, ShardStrategy shardStrategy, string indexName, string[] fieldsToFetch, string[] projectionFields, IDocumentQueryListener[] queryListeners, bool isMapReduce)
			: base(session
#if !SILVERLIGHT
, null
#endif
, null, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce)
		{
			this.getShardsToOperateOn = getShardsToOperateOn;
			this.shardStrategy = shardStrategy;
		}

		protected override Task<QueryOperation> InitAsync()
		{
			if (queryOperation != null)
				return CompletedTask.With(queryOperation);

			ExecuteBeforeQueryListeners();

			shardQueryOperations = new List<QueryOperation>();

			foreach (var commands in ShardDatabaseCommands)
			{
				var dbCommands = commands;
				ClearSortHints(dbCommands);
				shardQueryOperations.Add(InitializeQueryOperation((key, val) => dbCommands.OperationsHeaders[key] = val));
			}

			theSession.IncrementRequestCount();
			return ExecuteActualQueryAsync();
		}

		public override IAsyncDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections)
		{
			var documentQuery = new AsyncShardedDocumentQuery<TProjection>(theSession,
				getShardsToOperateOn,
				shardStrategy,
				indexName,
				fields,
				projections,
				queryListeners,
				isMapReduce)
			{
				pageSize = pageSize,
				queryText = new StringBuilder(queryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				queryStats = queryStats,
				theWaitForNonStaleResults = theWaitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				isDistinct = isDistinct,
				transformResultsFunc = transformResultsFunc,
				includes = new HashSet<string>(includes),
				highlightedFields = new List<HighlightedField>(highlightedFields),
				highlighterPreTags = highlighterPreTags,
				highlighterPostTags = highlighterPostTags,
				disableEntitiesTracking = disableEntitiesTracking,
				disableCaching = disableCaching,
				shouldExplainScores = shouldExplainScores
			};
			documentQuery.AfterQueryExecuted(afterQueryExecutedCallback);
			return documentQuery;
		}

#if !SILVERLIGHT && !NETFX_CORE

		protected override void ExecuteActualQuery()
		{
			throw new NotSupportedException("Async queries don't support synchronous execution");
		}

#endif

		protected override Task<QueryOperation> ExecuteActualQueryAsync()
		{
			var results = CompletedTask.With(new bool[ShardDatabaseCommands.Count]).Task;

			Func<Task> loop = null;
			loop = () =>
			{
				var lastResults = results.Result;

				results = shardStrategy.ShardAccessStrategy.ApplyAsync(ShardDatabaseCommands,
					new ShardRequestData
					{
						EntityType = typeof(T),
						Query = IndexQuery,
						IndexName = indexName
					}, (commands, i) =>
					{
						if (lastResults[i]) // if we already got a good result here, do nothing
							return CompletedTask.With(true);

						var queryOp = shardQueryOperations[i];

						var queryContext = queryOp.EnterQueryContext();
						return commands.QueryAsync(indexName, queryOp.IndexQuery, includes.ToArray())
							.ContinueWith(task =>
						{
							if (queryContext != null)
								queryContext.Dispose();

							return queryOp.IsAcceptable(task.Result);
						});
					});

				return results.ContinueWith(task =>
				{
					task.AssertNotFailed();

					if (lastResults.All(acceptable => acceptable))
						return new CompletedTask().Task;


					ThreadSleep.Sleep(100);

					return loop();
				}).Unwrap();
			};

			return loop().ContinueWith(task =>
			{
				task.AssertNotFailed();

#if !NETFX_CORE && !SILVERLIGHT
				ShardedDocumentQuery<T>.AssertNoDuplicateIdsInResults(shardQueryOperations);
#endif

				var mergedQueryResult = shardStrategy.MergeQueryResults(IndexQuery, shardQueryOperations.Select(x => x.CurrentQueryResults).ToList());

				shardQueryOperations[0].ForceResult(mergedQueryResult);
				queryOperation = shardQueryOperations[0];

				return queryOperation;
			});
		}

#if !NETFX_CORE
		public override Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
		{
			throw new NotSupportedException("Lazy in not supported with the async API");
		}
#endif

		public override IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}

		public override IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}
	}
}
#endif