#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ShardStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardQuery;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Client.Shard.ShardStrategy
{
	/// <summary>
	/// Default shard strategy for the sharding document store
	/// </summary>
	public class ShardStrategy : IShardStrategy
	{
		/// <summary>
		/// Gets or sets the shard selection strategy.
		/// </summary>
		public IShardSelectionStrategy ShardSelectionStrategy { get; set; }
		/// <summary>
		/// Gets or sets the shard resolution strategy.
		/// </summary>
		public IShardResolutionStrategy ShardResolutionStrategy { get; set; }
		/// <summary>
		/// Gets or sets the shard access strategy.
		/// </summary>
		public IShardAccessStrategy ShardAccessStrategy { get; set; }

		/// <summary>
		/// Get the shard query startegy
		/// </summary>
		public IShardQueryStrategy ShardQueryStrategy { get; set; }
	}
}
#endif
