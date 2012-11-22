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
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Document.Batches;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;
using Raven.Client.Shard;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	public class ShardedDocumentQuery<T> : DocumentQuery<T>
	{
		private readonly Func<ShardRequestData, IList<Tuple<string, IDatabaseCommands>>> getShardsToOperateOn;
		private readonly ShardStrategy shardStrategy;
		private List<QueryOperation> shardQueryOperations;
		
		private IList<IDatabaseCommands> databaseCommands;
		private IList<IDatabaseCommands> ShardDatabaseCommands
		{
			get
			{
				if (databaseCommands == null)
				{
					var shardsToOperateOn = getShardsToOperateOn(new ShardRequestData {EntityType = typeof (T), Query = IndexQuery, IndexName = indexName});
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
		public ShardedDocumentQuery(InMemoryDocumentSessionOperations session, Func<ShardRequestData, IList<Tuple<string, IDatabaseCommands>>> getShardsToOperateOn, ShardStrategy shardStrategy, string indexName, string[] fieldsToFetch, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
			, null
#endif
			, null
			, indexName, 
			fieldsToFetch,
			projectionFields, 
			queryListeners)
		{
			this.getShardsToOperateOn = getShardsToOperateOn;
			this.shardStrategy = shardStrategy;
		}

		protected override void InitSync()
		{
			if (queryOperation != null)
				return;

			shardQueryOperations = new List<QueryOperation>();
			theSession.IncrementRequestCount();

			ExecuteBeforeQueryListeners();

			foreach (var dbCmd in ShardDatabaseCommands)
			{
				ClearSortHints(dbCmd);
				shardQueryOperations.Add(InitializeQueryOperation(dbCmd.OperationsHeaders.Add));
			}

			ExecuteActualQuery();
		}

		public override IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections)
		{
			var documentQuery = new ShardedDocumentQuery<TProjection>(theSession,
				getShardsToOperateOn,
				shardStrategy, 
				indexName,
				fields,
				projections,
				queryListeners)
			{
				pageSize = pageSize,
				queryText = new StringBuilder(queryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				cutoffEtag = cutoffEtag,
				queryStats = queryStats,
				theWaitForNonStaleResults = theWaitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				groupByFields = groupByFields,
				aggregationOp = aggregationOp,
				transformResultsFunc = transformResultsFunc,
				includes = new HashSet<string>(includes),
				rootTypes = {typeof(T)}
			};
			documentQuery.AfterQueryExecuted(afterQueryExecutedCallback);
			return documentQuery;
		}

		protected override void ExecuteActualQuery()
		{
			var results = new bool[ShardDatabaseCommands.Count];
			while (true)
			{
				var currentCopy = results;
				results = shardStrategy.ShardAccessStrategy.Apply(ShardDatabaseCommands,
					new ShardRequestData
					{
						EntityType = typeof(T),
						Query = IndexQuery,
						IndexName = indexName
					}, (dbCmd, i) =>
				{
					if (currentCopy[i]) // if we already got a good result here, do nothing
						return true;

					var queryOp = shardQueryOperations[i];

					using (queryOp.EnterQueryContext())
					{
						queryOp.LogQuery();
						var result = dbCmd.Query(indexName, queryOp.IndexQuery, includes.ToArray());
						return queryOp.IsAcceptable(result);
					}
				});
				if (results.All(acceptable => acceptable))
					break;
				Thread.Sleep(100);
			}

			AssertNoDuplicateIdsInResults(shardQueryOperations);

			var mergedQueryResult = shardStrategy.MergeQueryResults(IndexQuery,
			                                                        shardQueryOperations.Select(x => x.CurrentQueryResults)
			                                                        	.Where(x => x != null)
			                                                        	.ToList());

			shardQueryOperations[0].ForceResult(mergedQueryResult);
			queryOperation = shardQueryOperations[0];
			
			afterQueryExecutedCallback(mergedQueryResult);
		}

		internal static void AssertNoDuplicateIdsInResults(List<QueryOperation> shardQueryOperations)
		{
			var shardsPerId = new Dictionary<string, HashSet<QueryOperation>>(StringComparer.InvariantCultureIgnoreCase);

			foreach (var shardQueryOperation in shardQueryOperations)
			{
				var currentQueryResults = shardQueryOperation.CurrentQueryResults;
				if(currentQueryResults == null)
					continue;
				foreach (var include in currentQueryResults.Includes.Concat(currentQueryResults.Results))
				{
					var includeMetadata = include.Value<RavenJObject>(Constants.Metadata);
					if(includeMetadata == null)
						continue;
					var id = includeMetadata.Value<string>("@id");
					if(id == null)
						continue;
					shardsPerId.GetOrAdd(id).Add(shardQueryOperation);
				}
			}

			foreach (var shardPerId in shardsPerId)
			{
				if (shardPerId.Value.Count > 1)
					throw new InvalidOperationException("Found id: " + shardPerId.Key + " on more than one shard, documents ids must be unique cluster-wide.");
			}
		}

#if !SILVERLIGHT
		/// <summary>
		///   Grant access to the database commands
		/// </summary>
		public override IDatabaseCommands DatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}
#endif

		/// <summary>
		///   Grant access to the async database commands
		/// </summary>
		public override IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { throw new NotSupportedException("Sharded has more than one DatabaseCommands to operate on."); }
		}

#if !SILVERLIGHT

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public override Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
		{
			if (queryOperation == null)
			{
				foreach (var databaseCommands11 in ShardDatabaseCommands)
				{
					foreach (var key in databaseCommands11.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
					{
						databaseCommands11.OperationsHeaders.Remove(key);
					}
				}
			
				ExecuteBeforeQueryListeners();
				queryOperation = InitializeQueryOperation((s, s1) => ShardDatabaseCommands.ForEach(cmd => cmd.OperationsHeaders.Set(s, s1)));
			}

			var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback, includes);

			return ((ShardedDocumentSession)theSession).AddLazyOperation(lazyQueryOperation, onEval, ShardDatabaseCommands);
		}
#endif

		protected override Task<QueryOperation> ExecuteActualQueryAsync()
		{
			throw new NotSupportedException();
		}
	}
}
#endif