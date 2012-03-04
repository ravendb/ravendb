//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using System.Threading;
#if !NET_3_5
using System.Threading.Tasks;
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
	public class ShardedDocumentQuery<T> : DocumentQuery<T>
	{
		private readonly Func<ShardRequestData, IList<IDatabaseCommands>> getShardsToOperateOn;
		private readonly IShardStrategy shardStrategy;
		private List<QueryOperation> shardQueryOperations;
		private IList<IDatabaseCommands> databaseCommands;
		private IndexQuery indexQuery;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery{T}"/> class.
		/// </summary>
		public ShardedDocumentQuery(InMemoryDocumentSessionOperations session, Func<ShardRequestData, IList<IDatabaseCommands>> getShardsToOperateOn, IShardStrategy shardStrategy, string indexName, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
			, null
#endif
#if !NET_3_5
			, null
#endif
			, indexName, projectionFields, queryListeners)
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

			indexQuery = GenerateIndexQuery(theQueryText.ToString());

			databaseCommands = getShardsToOperateOn(new ShardRequestData{EntityType = typeof(T), Query = indexQuery});
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

			var shardIds = shardStrategy.ShardResolutionStrategy.PotentialShardsFor(new ShardRequestData
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