// -----------------------------------------------------------------------
//  <copyright file="EtagSynchronizerType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Impl.Synchronization
{
	public enum EtagSynchronizerType
	{
		Indexer = 1,
		Reducer = 2,
		Replicator = 3,
		SqlReplicator = 4
	}
}