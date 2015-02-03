// -----------------------------------------------------------------------
//  <copyright file="IndexingWorkStatsSchema.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions.StructureSchemas
{
	public enum IndexingWorkStatsFields
	{
		IndexingAttempts,
		IndexingSuccesses,
		IndexingErrors,
		LastIndexingTime,
		IndexId,
		CreatedTimestamp
	}
}