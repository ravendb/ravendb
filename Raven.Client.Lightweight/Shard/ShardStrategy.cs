//-----------------------------------------------------------------------
// <copyright file="ShardStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Client.Shard.ShardAccess;
using Raven.Client.Shard.ShardResolution;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Default shard strategy for the sharding document store
	/// </summary>
	public class ShardStrategy
	{
		public delegate QueryResult MergeQueryResultsFunc(IndexQuery query, IList<QueryResult> queryResults);

		public ShardStrategy()
		{
			ShardAccessStrategy = new SequentialShardAccessStrategy();
			MergeQueryResults = DefaultMergeQueryResults;
		}

		/// <summary>
		/// Merge the query results from all the shards into a single query results object
		/// </summary>
		public MergeQueryResultsFunc MergeQueryResults { get; set; }

		/// <summary>
		/// Merge the query results from all the shards into a single query results object by simply
		/// concatenating all of the values
		/// </summary>
		public QueryResult DefaultMergeQueryResults(IndexQuery query, IList<QueryResult> queryResults)
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
				Includes = queryResults.SelectMany(x => x.Includes).ToList(),
				Results = queryResults.SelectMany(x => x.Results).ToList(),

				IndexName = queryResults.Select(x => x.IndexName).FirstOrDefault(),
				IndexTimestamp = queryResults.Select(x => x.IndexTimestamp).OrderBy(x => x).FirstOrDefault(),
				IsStale = queryResults.Any(x => x.IsStale),
				TotalResults = queryResults.Sum(x => x.TotalResults),
				IndexEtag = indexEtag,
				SkippedResults = queryResults.Select(x => x.SkippedResults).OrderBy(x => x).FirstOrDefault(),
			};
		}

		/// <summary>
		/// Gets or sets the shard resolution strategy.
		/// </summary>
		public IShardResolutionStrategy ShardResolutionStrategy { get; set; }
		/// <summary>
		/// Gets or sets the shard access strategy.
		/// </summary>
		public IShardAccessStrategy ShardAccessStrategy { get; set; }
	}
}
#endif