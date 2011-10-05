using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using System.Linq;
#if SILVERLIGHT
using Raven.Client.Silverlight.MissingFromSilverlight;
#endif 
namespace Raven.Client.Shard.ShardStrategy.ShardQuery
{
	/// <summary>
	/// Simply merge all of the results from all of the queries
	/// </summary>
	public class SimpleMergingShardQueryStrategy : IShardQueryStrategy
	{
		/// <summary>
		/// Merge the query results from all the shards into a single query results object
		/// </summary>
		public QueryResult MergeQueryResults(IndexQuery query, IList<QueryResult> queryResults, IList<string> shardIds)
		{
			var buffer = queryResults.SelectMany(x => x.IndexEtag.ToByteArray()).ToArray();
			Guid indexEtag;
#if !SILVERLIGHT
			using (var md5 = MD5.Create())
			{
				indexEtag = new Guid(md5.ComputeHash(buffer));
			}
#else
			indexEtag = new Guid(MD5Core.GetHash(buffer));

#endif

			return new QueryResult
			{
				Includes = queryResults.SelectMany(x=>x.Includes).ToList(),
				Results = queryResults.SelectMany(x => x.Results).ToList(),

				IndexName = queryResults.Select(x=>x.IndexName).FirstOrDefault(),
				IndexTimestamp = queryResults.Select(x=>x.IndexTimestamp).OrderBy(x=>x).FirstOrDefault(),
				IsStale = queryResults.Any(x=>x.IsStale),
				TotalResults = queryResults.Sum(x=>x.TotalResults),
				IndexEtag = indexEtag,
				SkippedResults = queryResults.Select(x=>x.SkippedResults).OrderBy(x=>x).FirstOrDefault(),
			};
		}

		
	}
}