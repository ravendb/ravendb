// -----------------------------------------------------------------------
//  <copyright file="PrefetchingUser.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Prefetching
{
	public enum PrefetchingUser
	{
		Indexer = 1,
		Replicator = 2,
		SqlReplicator = 3,
	}
}