//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
#if !NET_3_5
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document.Batches;
#endif
using Raven.Client.Document.SessionOperations;
using Raven.Client.Listeners;
using Raven.Client.Connection;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardResolution;

#if !SILVERLIGHT
namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class ShardedDocumentQuery<T> : DocumentQuery<T>
	{
		public delegate IList<IDatabaseCommands> SelectShardsDelegate(Type type, IndexQuery query);
		private readonly SelectShardsDelegate selectShards;
		private readonly IShardStrategy shardStrategy;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		public ShardedDocumentQuery(InMemoryDocumentSessionOperations session, SelectShardsDelegate selectShards,
			IShardStrategy shardStrategy, 
			string indexName, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
			, null
#endif
#if !NET_3_5
			, null
#endif
			, indexName, projectionFields, queryListeners)
		{
			if (selectShards == null)
				throw new ArgumentNullException("selectShards");

			this.selectShards = selectShards;
			this.shardStrategy = shardStrategy;
		}

		List<QueryOperation> shardQueryOperations;
		IList<IDatabaseCommands> databaseCommands;
		IndexQuery indexQuery;

		protected override void InitSync()
		{
			if (queryOperation != null)
				return;

			shardQueryOperations = new List<QueryOperation>();
			theSession.IncrementRequestCount();

			ExecuteBeforeQueryListeners();

			indexQuery = GenerateIndexQuery(theQueryText.ToString());

			databaseCommands = selectShards(typeof (T), indexQuery);

			foreach (var dbCmd in databaseCommands)
			{
				ClearSortHints(dbCmd);
				shardQueryOperations.Add(InitializeQueryOperation(dbCmd.OperationsHeaders.Add));
			}

			ExecuteActualQuery();
		}


		protected override void ExecuteActualQuery()
		{
			IList<bool> results = new List<bool>(databaseCommands.Count);
			while (true)
			{
				IList<bool> currentCopy = results;
				results = shardStrategy.ShardAccessStrategy.Apply(databaseCommands, (dbCmd, index) =>
				{
					if (currentCopy[index])
						return true;

					var queryOp = shardQueryOperations[index];

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

			var shardIds = shardStrategy.ShardResolutionStrategy.SelectShardIds(new ShardResolutionStrategyData
			{
				EntityType = typeof(T),
				Query = indexQuery
			});
			var mergedQueryResult = shardStrategy.ShardQueryStrategy.MergeQueryResults(indexQuery,
																						shardQueryOperations.Select(x => x.CurrentQueryResults).ToList(), 
			                                                                            shardIds);

			shardQueryOperations[0].ForceResult(mergedQueryResult);
			queryOperation = shardQueryOperations[0];
		}
		
#if !NET_3_5
		protected override Task<QueryOperation> ExecuteActualQueryAsync()
		{
			throw new NotSupportedException();
		}
#endif
	}
}
#endif