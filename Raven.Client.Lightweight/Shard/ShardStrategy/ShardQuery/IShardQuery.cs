using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Shard.ShardStrategy.ShardQuery
{
	/// <summary>
	/// Implementers of this interface provide a way to decide how to merge queries from multiple sources
	/// </summary>
	public interface IShardQueryStrategy
	{
		/// <summary>
		/// Merge the query results from all the shards into a single query results object
		/// </summary>
		QueryResult MergeQueryResults(IndexQuery query, IList<QueryResult> queryResults, IList<string> shardIds);
	}
}