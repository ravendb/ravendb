//-----------------------------------------------------------------------
// <copyright file="IShardSelectionStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client.Shard.ShardStrategy.ShardSelection
{
	/// <summary>
	/// Implementors of this interface provide a way to go from an exists/new
	/// entity to the appropriate shard for the entity
	/// </summary>
    public interface IShardSelectionStrategy
    {
		/// <summary>
		/// Find the shard id for a new object.
		/// </summary>
        string ShardIdForNewObject(object obj);
		/// <summary>
		/// Find the shard id for existing object.
		/// </summary>
        string ShardIdForExistingObject(object obj);
    }
}
