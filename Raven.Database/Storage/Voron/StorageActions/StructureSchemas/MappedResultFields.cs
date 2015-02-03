// -----------------------------------------------------------------------
//  <copyright file="MappedResultsFields.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions.StructureSchemas
{
	public enum MappedResultFields
	{
		IndexId,
		Bucket,
		Timestamp,
		ReduceKey,
		DocId,
		Etag
	}
}