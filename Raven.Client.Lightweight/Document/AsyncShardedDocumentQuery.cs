//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;
using Raven.Client.Shard;
using Raven.Client.Extensions;

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
					var shardsToOperateOn = getShardsToOperateOn(new ShardRequestData { EntityType = typeof(T), Query = IndexQuery });
					databaseCommands = shardsToOperateOn.Select(x => x.Item2).ToList();
				}
				return databaseCommands;
			}
		}

		private IndexQuery indexQuery;
		private IndexQuery IndexQuery
		{
			get { return indexQuery ?? (indexQuery = GenerateIndexQuery(theQueryText.ToString())); }
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery{T}"/> class.
		/// </summary>
		public AsyncShardedDocumentQuery(InMemoryDocumentSessionOperations session, Func<ShardRequestData, IList<Tuple<string, IAsyncDatabaseCommands>>> getShardsToOperateOn, ShardStrategy shardStrategy, string indexName, string[] fieldsToFetch, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
, null
#endif
, null, indexName, fieldsToFetch, projectionFields, queryListeners)
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
				queryListeners)
			{
				pageSize = pageSize,
				theQueryText = new StringBuilder(theQueryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				queryStats = queryStats,
				theWaitForNonStaleResults = theWaitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				groupByFields = groupByFields,
				aggregationOp = aggregationOp,
				transformResultsFunc = transformResultsFunc,
				includes = new HashSet<string>(includes),
			};
			documentQuery.AfterQueryExecuted(afterQueryExecutedCallback);
			return documentQuery;
		}

		protected override void ExecuteActualQuery()
		{
			throw new NotSupportedException("Async queries don't support synchronous execution");
		}

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
						Query = IndexQuery
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

					Thread.Sleep(100);

					return loop();
				}).Unwrap();
			};

			return loop().ContinueWith(task =>
			{
				task.AssertNotFailed();

				ShardedDocumentQuery<T>.AssertNoDuplicateIdsInResults(shardQueryOperations);

				var mergedQueryResult = shardStrategy.MergeQueryResults(IndexQuery, shardQueryOperations.Select(x => x.CurrentQueryResults).ToList());

				shardQueryOperations[0].ForceResult(mergedQueryResult);
				queryOperation = shardQueryOperations[0];

				return queryOperation;
			});
		}

		public override Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
		{
			throw new NotSupportedException("Lazy in not supported with the async API");
		}

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
