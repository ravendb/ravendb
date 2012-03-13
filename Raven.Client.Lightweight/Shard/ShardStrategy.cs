//-----------------------------------------------------------------------
// <copyright file="ShardStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Default shard strategy for the sharding document store
	/// </summary>
	public class ShardStrategy
	{
		private readonly IDictionary<string, IDocumentStore> shards;

		public delegate QueryResult MergeQueryResultsFunc(IndexQuery query, IList<QueryResult> queryResults);

		public delegate string ModifyDocumentIdFunc(DocumentConvention convention, string shardId, string documentId);

		public ShardStrategy(IDictionary<string, IDocumentStore> shards)
		{
			this.shards = new Dictionary<string, IDocumentStore>(shards, StringComparer.InvariantCultureIgnoreCase);

			if (shards.Count == 0)
				throw new ArgumentException("Shards collection must have at least one item", "shards");

			Conventions = shards.First().Value.Conventions.Clone();

			ShardAccessStrategy = new SequentialShardAccessStrategy();
			ShardResolutionStrategy = new DefaultShardResolutionStrategy(shards.Keys, this);
			MergeQueryResults = DefaultMergeQueryResults;
			ModifyDocumentId = (convention, shardId, documentId) => shardId + convention.IdentityPartsSeparator + documentId;
		}

		public DocumentConvention Conventions { get; set; }

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

		/// <summary>
		/// Get or sets the modification for the document id for sharding
		/// </summary>
		public ModifyDocumentIdFunc ModifyDocumentId { get; set; }

		public IDictionary<string, IDocumentStore> Shards
		{
			get { return shards; }
		}

		public ShardStrategy ShardingOn<TEntity>(Expression<Func<TEntity, string>> shardingProperty,
			Func<string, string> translator = null
		)
		{
			var defaultShardResolutionStrategy = ShardResolutionStrategy as DefaultShardResolutionStrategy;
			if (defaultShardResolutionStrategy == null)
				throw new NotSupportedException("ShardingOn<T> is only supported if ShardResulotionStrategy is DefaultShardResolutionStrategy");

			defaultShardResolutionStrategy.ShardingOn(shardingProperty, translator);
			return this;
		}

		public ShardStrategy  ShardingOn<TEntity, TResult>(Expression<Func<TEntity, TResult>> shardingProperty, 
			Func<TResult, string> valueTranslator = null,
			Func<string, string> queryTranslator = null
			)
		{
			var defaultShardResolutionStrategy = ShardResolutionStrategy as DefaultShardResolutionStrategy;
			if (defaultShardResolutionStrategy == null)
				throw new NotSupportedException("ShardingOn<T> is only supported if ShardResulotionStrategy is DefaultShardResolutionStrategy");

			defaultShardResolutionStrategy.ShardingOn(shardingProperty, valueTranslator, queryTranslator);
			return this;
		
		}
	}
}

#endif