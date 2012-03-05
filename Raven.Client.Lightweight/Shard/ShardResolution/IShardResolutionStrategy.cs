//-----------------------------------------------------------------------
// <copyright file="IShardResolutionStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Client.Shard.ShardResolution
{
	/// <summary>
	/// Implementers of this interface provide a way to decide which shards will be queried
	/// for a specified operation
	/// </summary>
	public interface IShardResolutionStrategy
	{
		/// <summary>
		/// Generate a shard id for the specified entity
		/// </summary>
		string GenerateShardIdFor(object entity);

		/// <summary>
		/// The shard id for the server that contains the metadata (such as the hilo documents)
		/// for the given entity
		/// </summary>
		string MetadataShardIdFor(object entity);

		/// <summary>
		/// Selects the shard ids appropriate for the specified data
		/// </summary>
		/// <remarks>
		/// Returning null means search all shards
		/// </remarks>
		IList<string> PotentialShardsFor(ShardRequestData requestData);
	}
}